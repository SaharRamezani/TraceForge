use crate::event::Event;
use crate::event_label::{AsEventLabel, Inbox, LabelEnum, RecvMsg, SendMsg};
use crate::future::PollerMsg;
use crate::exec_graph::ExecutionGraph;
use crate::loc::CommunicationModel;
use crate::revisit::Revisit;
use crate::vector_clock::VectorClock;
use crate::loc::WakeMsg;
use crate::timed_cons::{TimedConfig, WaitTime};
use crate::timed_dcs::TimedDcs;
use log::debug;

// A generic consistency which will, eventually, support arbitrary
// communication models, depending on the channel.

// The intended semantics of consistent(G) is
// 1. porf-acyclic(G)
// 2. for each communication model M, the restriction of G
// to events of models not stronger than M is consistent under M
// 3. receive events of monitors act as if they are CausalOrder

pub(crate) struct Consistency {}

impl Consistency {
    // Checks if there is a TotalOrder relation between the two sends slab1 and slab2
    fn send_before(&self, g: &ExecutionGraph, slab1: Event, slab2: Event) -> bool {
        // Apart from slab1, also do not query send_before(slab2, slab2)
        Self::aux_send_before(g, slab1, slab2, &mut vec![slab1, slab2])
    }

    // send_before = (porf U induced_send_before)^+
    fn aux_send_before(
        g: &ExecutionGraph,
        slab1: Event,
        // Every recursive call uses the same slab2
        slab2: Event,
        // Events s that we shouldn't query for send_before(s, slab2),
        // either because we are already (nested) in the process of answering the query
        // or because we already know that the query returns false
        seen: &mut Vec<Event>,
    ) -> bool {
        // porf <= send_before
        if g.send_label(slab2).unwrap().porf().contains(slab1) {
            return true;
        }

        // Transitivity: [slab1];send_before;[slab];send_before <= send_before
        for slab in g.all_store_iter() {
            // Only use TotalOrder sends as transitive steps
            if slab.comm() != CommunicationModel::TotalOrder {
                continue;
            }

            // Avoid recursing on send_before(s, s2), for some s that has already been tried
            if seen.contains(&slab.pos()) {
                continue;
            }

            // Check if [slab1];porf;[slab];send_before;[slab2]
            if slab.porf().contains(slab1) {
                seen.push(slab.pos());
                if Self::aux_send_before(g, slab.pos(), slab2, seen) {
                    return true;
                }
            }

            // Check if [slab1];induced_send_before;[slab];send_before;[slab2]
            // where (s1, s2) in induced_send_before iff s1 is read by a r1 that also matches s2 and
            // s2 is not read by an earlier receive r2.
            // Since there are no concurrent receives, "later" means porf.

            // slab1 is read
            let rlab1 = match g.send_label(slab1).unwrap().reader() {
                Some(rlab1) => g.recv_label(rlab1).unwrap(),
                None => continue,
            };

            // by a receive rlab1 that could also read slab
            if !rlab1.matches(slab) {
                continue;
            }

            // slab is not read, or is read by rlab s.t. (rlab1, rlab) in porf
            if slab
                .reader()
                .is_none_or(|rlab| g.in_porf(rlab1.pos(), rlab))
            {
                seen.push(slab.pos());
                if Self::aux_send_before(g, slab.pos(), slab2, seen) {
                    return true;
                }
            }
        }
        false
    }

    /// Returns the subset of the sends s.t. they can be read from rlab after (possibly) restricting the graph to the view.
    /// The view implicitly excludes one event: View = (VectorClock, excluded Event)
    /// Lack of view implies we consider the whole graph.
    fn filter_available_sends_in_view<'a>(
        g: &'a ExecutionGraph,
        rlab: &'a RecvMsg,
        sends: impl Iterator<Item = &'a SendMsg>,
        view: Option<(&'a VectorClock, Option<Event>)>,
        check_concurrent: bool,
    ) -> impl Iterator<Item = &'a SendMsg> {
        // println!("====== Started filter sends call");
        let rpos = rlab.pos();
        sends.filter(move |&slab| {
            let spos = slab.pos();

            // exclude one event
            if view.is_some_and(|(_, excl)| excl.is_some_and(|ev| ev == spos)) {
                return false;
            }

            // send must be in the view, if it exists
            if view.is_some_and(|view| !view.0.contains(slab.pos())) {
                return false;
            }

            // *Assumption*: if this send message is monitored by the receive's thread,
            // it cannot be that it is a send message towards the monitor itself.
            if slab.is_monitored_from(&rpos.thread) {
                !slab.monitor_readers().iter().any(|&reader| {
                    // As long as it is not monitor-read by an same-thread (same-monitor)
                    // event that would remain in the view, the send can be monitor-read.
                    //
                    // Exclude the receive itself
                    reader != rpos
                        && reader.thread == rpos.thread
                        && view.is_none_or(|view| view.0.contains(reader))
                })
            } else {
                // there is no reader, or the reader is *not* in the view (we exclude the receive itself)
                debug!("Started looking at send {}", slab);
                match slab.reader() {
                    None => true,
                    Some(reader) => {
                        debug!("He is received at {:?}", reader);
                        // Check for concurrent receives.
                        // We shouldn't include the receive's rf, i.e. use cached_porf.

                        // N.B. it should suffice to check only when we add rlab
                        // (it's the last event in its thread).
                        // Otherwise, we should also check whether
                        // rlab is before reader OR reader is bofore rlab
                        if check_concurrent && !rlab.cached_porf().contains(reader) {
                            println!("{}", g);
                            panic!(
                                "Detected concurrent receives: {} and {}",
                                reader,
                                rlab.pos()
                            );
                        }
                        reader == rpos
                            || view.is_some_and(|view| !view.0.contains(reader))
                            // A send is available if its reader was part of an async
                            // receive that was subsequently cancelled.
                            || {
                                let cancel_available =
                                    Self::reader_cancelled_async(g, slab, reader, view);
                                if cancel_available {
                                    debug!("[cancel_path] send {} (reader={}) made available via cancel path", spos, reader);
                                    slab.push_cancelled_recv_reader(reader);
                                }
                                cancel_available
                            }
                    }
                }
            }
        })
    }

    fn filter_available_sends_in_view_for_inbox<'a>(
        g: &'a ExecutionGraph,
        ilab: &'a Inbox,
        sends: impl Iterator<Item = &'a SendMsg>,
        view: Option<(&'a VectorClock, Option<Event>)>,
        check_concurrent: bool,
    ) -> impl Iterator<Item = &'a SendMsg> {
        let rpos = ilab.pos();
        sends.filter(move |&slab| {
            let spos = slab.pos();

            // Revisit view can explicitly exclude one send.
            if view.is_some_and(|(_, excl)| excl.is_some_and(|ev| ev == spos)) {
                return false;
            }

            // Keep only sends present in the chosen prefix view.
            if view.is_some_and(|view| !view.0.contains(slab.pos())) {
                return false;
            }

            match slab.reader() {
                None => true,
                Some(reader) => {
                    // Same concurrency sanity check as plain receives.
                    if check_concurrent && !ilab.cached_porf().contains(reader) {
                        println!("{}", g);
                        panic!(
                            "Detected concurrent receives: {} and {}",
                            reader,
                            ilab.pos()
                        );
                    }
                    // Keep send if it is still unread in the view, or already read by this inbox.
                    reader == rpos || view.is_some_and(|view| !view.0.contains(reader))
                }
            }
        })
    }

    /// Returns whether the send has no sb-predecessor (porf-predecessors if flag is set) among the rest sends
    fn is_sb_miminal(send: &SendMsg, sends: &[&SendMsg], porf_override: bool) -> bool {
        let view = if porf_override {
            send.porf()
        } else {
            send.sb()
        };
        !sends.iter().any(|&e| view.contains(e.pos()))
    }

    /// Is `slab`'s `reader` of an async receive
    /// that was cancelled (making the send re-available)?
    /// Excludes internal PollerMsg/WakeMsg sends.
    pub(crate) fn reader_cancelled_async(
        g: &ExecutionGraph,
        slab: &SendMsg,
        reader: Event,
        view: Option<(&VectorClock, Option<Event>)>,
    ) -> bool {
        if slab.val.as_any_ref().downcast_ref::<PollerMsg>().is_some()
            || slab.val.as_any_ref().downcast_ref::<WakeMsg>().is_some()
        {
            return false;
        }
        g.get_thr(&reader.thread).labels[(reader.index as usize + 1)..]
            .iter()
            .any(|lab| {
                if let LabelEnum::RecvMsg(recv) = lab {
                    recv.rf().is_some_and(|rf| {
                        if let LabelEnum::SendMsg(send) = g.label(rf) {
                            send.val
                                .as_any_ref()
                                .downcast_ref::<PollerMsg>()
                                .is_some_and(|msg| matches!(msg, PollerMsg::Cancel))
                        } else {
                            false
                        }
                    }) && view.is_none_or(|view| view.0.contains(lab.pos()))
                } else {
                    false
                }
            })
    }

    /// GC eviction: send `b` is gone forever if
    /// once an in-view receive other than `current` consumed a send
    /// ordered after `b` on the same channel while `b` stayed unread.
    /// The reader must be one that could have read `b` (its predicate
    /// matches). Callers gate NoOrder (no order to invert) and
    /// TotalOrder (Mailbox is exempt from GC).
    pub(crate) fn send_overtaken_in_view(
        g: &ExecutionGraph,
        b: &SendMsg,
        current: Event,
        view: Option<(&VectorClock, Option<Event>)>,
    ) -> bool {
        // Overtakers are enumerated from b's OWN SENDER's subsequent
        // sends, NOT from the current receive's matching set
        let bpos = b.pos();
        g.get_thr(&bpos.thread).labels[(bpos.index as usize + 1)..]
            .iter()
            .any(|lab| {
                let LabelEnum::SendMsg(s2) = lab else {
                    return false;
                };
                if !s2.sb().contains(bpos) {
                    return false; // not ordered after b in b's channel
                }
                let Some(r2) = s2.reader() else {
                    return false;
                };
                if r2 == current {
                    return false;
                }
                if view.is_some_and(|v| !v.0.contains(r2)) {
                    return false;
                }
                // The reader must be one that COULD have read b (its
                // predicate matches b).
                match g.label(r2) {
                    LabelEnum::RecvMsg(rl) => rl.matches(b),
                    LabelEnum::Inbox(il) => il.matches(b),
                    _ => false,
                }
            })
    }

    /// uses it to skip its (then redundant) second probe pass.
    pub(crate) fn timed_gc_offer_arm(
        wait: Option<WaitTime>,
        comm: CommunicationModel,
    ) -> bool {
        wait.is_some() && comm != CommunicationModel::TotalOrder
    }

    /// Keeps the sb-minimals (porf-minimals is flag is set) among the (*stamp-ordered*) sends
    pub(crate) fn retain_sb_minimals<'a>(
        sends: impl Iterator<Item = &'a SendMsg>,
        porf_override: bool,
    ) -> Vec<&'a SendMsg> {
        // Among sends, stamp order respects porf, which includes sb for any model apart from TotalOrder.
        // Therefore, we can detect overwrites in a single forward pass.
        // Note: Amend this is we end up incrementally checking TotalOrder consistency as well.

        let mut sb_min = Vec::new();
        sends.for_each(|s| {
            if Self::is_sb_miminal(s, &sb_min, porf_override) {
                sb_min.push(s)
            }
        });
        sb_min
    }

    /// Returns the coherent matching stores that can be consistently read by recv
    /// when restricting the graph to the view (we exclude one event from the view).
    fn coherent_rfs_in_view(
        &self,
        g: &ExecutionGraph,
        // an optional view, excluding one event (a newly added send)
        view: Option<(&VectorClock, Option<Event>)>,
        recv: &RecvMsg,
        porf_override: bool,
        check_concurrent: bool,
        // Timed eligibility (GC semantics): with a
        // timed config, sends that can NEVER be read by this receive in any
        // consistent timeline are dropped BEFORE the communication-model
        // choice, so a time-dead front does not seal its channel. None =
        // untimed behaviour.
        timed: Option<&TimedConfig>,
    ) -> Vec<Event> {
        // Sends that the receive can read from
        let sends = g
            .matching_stores(recv.recv_loc())
            // filter-out WakeMsg in our porf prefix: the respective futures were cancelled
            .filter(|&s| !s.is_cancelled_wrt(recv.as_event_label()));

        // Keep those that will exist and be unread after the revisit, checking
        // for concurrent receives.
        let rfs = Self::filter_available_sends_in_view(g, recv, sends, view, check_concurrent);

        // Time-eligibility BEFORE the order choice (GC semantics), plus
        // GC eviction (an overtaken send is gone forever). Untimed
        // receives (wait = None) carry no timing constraints. NoOrder
        // gets the eligibility filter only.
        // TotalOrder (Mailbox) is exempt: its coherence is checked post
        // hoc by is_consistent. Mailbox channels keep seal semantics.
        let rfs: Vec<&SendMsg> = match timed {
            Some(cfg) if Self::timed_gc_offer_arm(recv.wait(), recv.comm()) => {
                let mut structurally: Vec<&SendMsg> = rfs.collect();
                if recv.comm() != CommunicationModel::NoOrder {
                    structurally.retain(|b| {
                        !Self::send_overtaken_in_view(g, b, recv.pos(), view)
                    });
                }
                if structurally.is_empty() {
                    structurally
                } else {
                    let vc = view.map(|(vc, _)| vc);
                    let mut dcs = TimedDcs::build(g, cfg, vc, Some(recv.pos()));
                    if dcs.base_feasible() {
                        structurally
                            .into_iter()
                            .filter(|s| dcs.probe_recv_rf(recv.pos(), s.pos()))
                            .collect()
                    } else {
                        // Engine-invariant breach tripwire: keep all
                        // (never tighter); certification gates reports.
                        structurally
                    }
                }
            }
            _ => rfs.collect(),
        };

        // Optional optimization for NoOrder
        let mut rfs: Vec<Event> = if recv.comm() != CommunicationModel::NoOrder {
            // *Assuming* there are no concurrent receives,
            // all existing matching receives are porf-before the current receives.
            // Therefore the consistent sends are exactly the sb-minimal ones.
            Self::retain_sb_minimals(rfs.into_iter(), porf_override)
                .iter()
                .map(|lab| lab.pos())
                .collect()
        } else {
            rfs.into_iter().map(|lab| lab.pos()).collect()
        };

        // Return them in an arbitrary but fixed order that does
        // *not* depend on the stamps.

        // This is the single place that uses Event's Ord constraint,
        // and *depends* on ThreadId's Ord implementation being stable
        // across executions (i.e. the underlying opaque_id not changing).
        // If this becomes a problem, one can recover a stable, deterministic,
        // ordering on ThreadId's from the execution graph:
        // consider the restriction to Create/Begin events, and use
        // e.g. a dfs pre-order for ordering TheadIds (and by extension, Events).
        rfs.sort();
        rfs
    }

    fn coherent_inbox_rfs_in_view(
        &self,
        g: &ExecutionGraph,
        view: Option<(&VectorClock, Option<Event>)>,
        inbox: &Inbox,
        check_concurrent: bool,
        timed: Option<&TimedConfig>,
        // Optional pre-built oracle to avoid a redundant build+solve.
        // PRECONDITION: built with view = None and floating = inbox.pos()
        // (only the full-graph offer path may inject; asserted below).
        oracle: Option<&mut TimedDcs>,
    ) -> Vec<Event> {
        // Candidate sends that match the inbox location/predicate.
        let sends = g.matching_stores(inbox.recv_loc());

        let rfs =
            Self::filter_available_sends_in_view_for_inbox(g, inbox, sends, view, check_concurrent);

        // Time-eligibility before the order choice (GC)
        let rfs: Vec<&SendMsg> = match timed {
            Some(cfg) if Self::timed_gc_offer_arm(inbox.wait(), inbox.comm()) => {
                let mut structurally: Vec<&SendMsg> = rfs.collect();
                if inbox.comm() != CommunicationModel::NoOrder {
                    structurally.retain(|b| {
                        !Self::send_overtaken_in_view(g, b, inbox.pos(), view)
                    });
                }
                if structurally.is_empty() {
                    structurally
                } else if let Some(dcs) = oracle {
                    // Shared oracle from the offer path (one build per
                    // inbox visit, reused for the joint subset probes).
                    debug_assert!(view.is_none(), "injected oracle requires the full-graph view");
                    if dcs.base_feasible() {
                        structurally
                            .into_iter()
                            .filter(|s| dcs.probe_inbox_rfs(inbox.pos(), &[s.pos()]))
                            .collect()
                    } else {
                        structurally
                    }
                } else {
                    let vc = view.map(|(vc, _)| vc);
                    let mut dcs = TimedDcs::build(g, cfg, vc, Some(inbox.pos()));
                    if dcs.base_feasible() {
                        structurally
                            .into_iter()
                            .filter(|s| dcs.probe_inbox_rfs(inbox.pos(), &[s.pos()]))
                            .collect()
                    } else {
                        structurally
                    }
                }
            }
            _ => rfs.collect(),
        };

        let mut rfs: Vec<Event> = if inbox.comm() != CommunicationModel::NoOrder {
            // Respect the channel's delivery model, mirroring recv behavior.
            Self::retain_sb_minimals(rfs.into_iter(), false)
                .iter()
                .map(|lab| lab.pos())
                .collect()
        } else {
            rfs.into_iter().map(|lab| lab.pos()).collect()
        };

        // Stable ordering for canonical subset derivation.
        rfs.sort();
        rfs
    }

    /// Calculates and populates necessary views for pos
    pub(crate) fn calc_views(&self, g: &mut ExecutionGraph, pos: Event) {
        if pos.index == 0 {
            let mut empty = VectorClock::new();
            empty.set_tid(pos.thread);
            g.label_mut(pos).set_porf_cache(empty.clone());
            g.label_mut(pos).set_posw_cache(empty.clone());
            return;
        }

        let prev = pos.prev();
        let mut porf = g.label(prev).cached_porf().clone();
        let mut posw = g.label(prev).cached_posw().clone();

        porf.update_idx(pos);
        posw.update_idx(pos);

        // Cached views do not include prev's direct dependencies (rf/TCreate/TEnd).
        // Adjust them to do so.

        // rf dependencies
        if let Some(rlab) = g.recv_label(prev) {
            if let Some(rf) = rlab.rf() {
                porf.update(g.label(rf).cached_porf());
                match rlab.comm() {
                    CommunicationModel::TotalOrder => { /* empty */ }
                    // posw does *not* include rf from TotalOrder events
                    _ => posw.update(g.label(rf).cached_posw()),
                }
            }
        }
        if let Some(ilab) = g.inbox_label(prev) {
            if let Some(rfs) = ilab.rfs() {
                for rf in rfs {
                    porf.update(g.label(rf).cached_porf());
                    match ilab.comm() {
                        CommunicationModel::TotalOrder => { /* empty */ }
                        // posw does *not* include rf from TotalOrder events
                        _ => posw.update(g.label(rf).cached_posw()),
                    }
                }
            }
        }

        // TCreate dependencies
        if let LabelEnum::Begin(blab) = g.label(prev) {
            if let Some(parent) = blab.parent() {
                porf.update(g.label(parent).cached_porf());
                // Create -> Begin contributes to sw as well
                posw.update(g.label(parent).cached_posw());
            }
        }

        // TEnd dependencies
        if let LabelEnum::TJoin(jlab) = g.label(prev) {
            porf.update(g.thread_last(jlab.cid()).unwrap().cached_porf());
            // Join -> End contributes to sw as well
            posw.update(g.thread_last(jlab.cid()).unwrap().cached_posw());
        }

        // Set send's sb view
        if let Some(slab) = g.send_label_mut(pos) {
            let mut sb = VectorClock::new();
            match slab.comm() {
                CommunicationModel::NoOrder => { /* empty */ }
                // Local: just include yourself (and po-predecessors)
                CommunicationModel::LocalOrder => sb.set(pos),
                CommunicationModel::CausalOrder => sb.update(&posw),
                // Treat Total similar to Causal, and check full consistency at the end
                CommunicationModel::TotalOrder => sb.update(&porf),
            }
            slab.set_sb(sb);
        }

        // Cache the views
        g.label_mut(pos).set_porf_cache(porf);
        g.label_mut(pos).set_posw_cache(posw);
    }

    pub(crate) fn is_consistent(&self, g: &ExecutionGraph) -> bool {
        for slab1 in g.all_store_iter() {
            if slab1.comm() != CommunicationModel::TotalOrder {
                continue;
            }
            for slab2 in g.all_store_iter() {
                if slab2.comm() != CommunicationModel::TotalOrder {
                    continue;
                }

                let s1 = slab1.pos();
                let s2 = slab2.pos();

                if s1 == s2 {
                    continue;
                }

                // For each pair (s1, s2) of sends with TotalOrder

                // s.t. s2 is read by a receive r2
                let r2 = match slab2.reader() {
                    None => continue,
                    Some(r2) => r2,
                };
                // that could have also read s1,
                if !g.recv_label(r2).unwrap().matches(slab1) {
                    continue;
                }

                // if s1 is read by a later (wrt r2) receive r1,
                if slab1.reader().is_some_and(|r1| g.in_porf(r1, r2)) {
                    continue;
                }

                // and s1 is causally_before s2,
                if self.send_before(g, s1, s2) {
                    // then the execution is inconsistent
                    return false;
                    // because s1 is ordered both
                    // - before s2 (send_before), and
                    // - after s2 (via their respective receives)
                }

                // N.B. We assumed that there are no concurrent receives
                // to reduce "r1 is earlier than r2" to "(r1, r2) in porf".
                // Otherwise, we need to explicitly enumerate linearizations
                // to judge consistency.
            }
        }
        true
    }

    /// Returns whether an affected receive is maximal during a revisit
    pub(crate) fn reads_tiebreaker(
        &self,
        g: &ExecutionGraph,
        rlab: &RecvMsg,
        rev: &Revisit,
        porf_override: bool,
        timed: Option<&TimedConfig>,
    ) -> bool {
        let (view, exclude) = match &rev.rev {
            crate::revisit::RevisitPlacement::Default(send) => {
                // rlab is not in the prefix of the revisitor
                assert!(!g.send_label(*send).unwrap().porf().contains(rlab.pos()));
                (
                    g.revisit_view(&Revisit::new(rlab.pos(), *send)),
                    Some(*send),
                )
            }
            crate::revisit::RevisitPlacement::Inbox(sends) => {
                let rev_inbox = Revisit::new_inbox(rlab.pos(), sends.clone());
                (g.revisit_view(&rev_inbox), None)
            }
            // Forward-only placement: never subject to backward-revisit
            // consistency checks.
            crate::revisit::RevisitPlacement::BlockInstead => unreachable!(),
        };
        // rlab is stamp greater or equal that revisitee's stamp
        assert!(rlab.stamp() >= g.label(rev.pos).stamp());

        // Nonblocking receives are maximal only when they timeout
        if rlab.is_non_blocking() {
            return rlab.rf().is_none();
        }

        // First (non-revisit) is the maximal one.
        // Or this reads from a send currently pointed to a different receive through
        // cancelled-reader fallback during replay.
        // Maximality must rank the SAME filtered list the offer
        // uses, or base choice and tiebreak disagree (duplicate/lost runs).
        let rfs =
            self.coherent_rfs_in_view(g, Some((&view, exclude)), rlab, porf_override, false, timed);
        if rfs.is_empty() {
            rlab.rf().is_some_and(|rf| {
                g.send_label(rf)
                    .is_some_and(|slab| slab.reader().is_some_and(|r| r != rlab.pos()))
            })
        } else {
            rlab.rf().unwrap() == rfs[0]
        }
    }

    pub(crate) fn inbox_reads_tiebreaker(
        &self,
        g: &ExecutionGraph,
        ilab: &Inbox,
        rev: &Revisit,
        timed: Option<&TimedConfig>,
    ) -> bool {
        // Non-blocking inbox (min==0): the maximal outcome is the immediate
        // empty. Only the untimed non-blocking inbox reaches this now (timed
        // inboxes require min >= 1); for it there is no timeout empty, so the
        // immediate empty `Some([])` is the sole maximal empty outcome.
        if ilab.is_non_blocking() {
            return matches!(ilab.rfs(), Some(rfs) if rfs.is_empty());
        }

        let Some(current) = ilab.rfs() else {
            // Timed-out inbox (rfs() == None). A finite-wait inbox that times
            // out is the canonical-maximal empty outcome of its branch: it is
            // the SINGLE graph from which the non-empty feasible subsets are
            // discovered by backward revisits (a finite inbox never blocks and
            // reruns, so there is no forward-enumeration alternative). Treat it
            // as maximal so Must::calc_revisits does not break before emitting
            // those revisits.
            //
            // Gate strictly on Finite: an untimed inbox (wait() == None) has no
            // timeout outcome, and an Infinite-wait inbox must block rather than
            // time out (timed_cons.rs Inbox arm returns an empty interval for
            // None + Infinite, and visit_inbox_rfs blocks instead of timing
            // out), so a None + Infinite label can never legitimately exist at a
            // completed graph; both stay non-maximal here.
            return matches!(ilab.wait(), Some(crate::timed_cons::WaitTime::Finite(_)));
        };

        if matches!(ilab.wait(), Some(crate::timed_cons::WaitTime::Finite(_))) {
            return false;
        }

        let view = g.revisit_view(rev);
        let exclude = match &rev.rev {
            // For recv-style revisit placement, remove the newly inserted send.
            crate::revisit::RevisitPlacement::Default(ev) => Some(*ev),
            // Inbox placement already names the whole candidate set in the revisit view.
            crate::revisit::RevisitPlacement::Inbox(_) => None,
            // Forward-only placement: never checked here.
            crate::revisit::RevisitPlacement::BlockInstead => unreachable!(),
        };

        let mut cands =
            self.coherent_inbox_rfs_in_view(g, Some((&view, exclude)), ilab, false, timed, None);

        Consistency::normalize_event_set(&mut cands);

        // Canonical subset in the revisit view: first `min` coherent sends.
        // Maximality requires the current inbox read to be exactly this subset.
        let limit = ilab.min().min(cands.len());
        let canonical: Vec<Event> = cands.into_iter().take(limit).collect();

        current == canonical
    }

    /// Returns the rf options for rlab, with the first being the non-revisit rf step
    pub(crate) fn rfs(
        &self,
        g: &ExecutionGraph,
        rlab: &RecvMsg,
        porf_override: bool,
        timed: Option<&TimedConfig>,
    ) -> Vec<Event> {
        self.coherent_rfs_in_view(g, None, rlab, porf_override, true, timed)
    }

    pub(crate) fn inbox_rfs(
        &self,
        g: &ExecutionGraph,
        ilab: &Inbox,
        timed: Option<&TimedConfig>,
        oracle: Option<&mut TimedDcs>,
    ) -> Vec<Event> {
        // Deterministic coherent inbox candidates for base execution / canonical subset.
        self.coherent_inbox_rfs_in_view(g, None, ilab, true, timed, oracle)
    }

    /// Returns whether the resulting execution would be consistent
    ///
    /// Assumes that rlab is not porf-before slab
    pub(crate) fn is_revisit_consistent(
        &self,
        g: &ExecutionGraph,
        rlab: &RecvMsg,
        slab: &SendMsg,
        porf_override: bool,
        timed: Option<&TimedConfig>,
    ) -> bool {
        assert!(rlab.matches(slab));

        let com = rlab.comm();

        // Optional optimization for NoOrder
        if com == CommunicationModel::NoOrder {
            return true;
        }

        // *Assuming* there are no concurrent receives
        // (which implies that the model is prefix-closed)
        // it suffices to check that slab is not overwritten in the resulting execution

        let rpos = rlab.pos();
        let spos = slab.pos();

        // We disregard the various communication models and check consistency
        // as if everything was CausalOrder.
        let send_sb = if porf_override {
            slab.porf()
        } else {
            slab.sb()
        };

        let view = g.revisit_view(&Revisit::new(rpos, spos));

        let sends = g.matching_stores(rlab.recv_loc()).filter(|&lab| {
            let pos = lab.pos();
            pos != spos && send_sb.contains(pos)
        });

        // if any of them, apart from slab, could be read by rlab after the revisit, then the execution is inconsistent
        let blockers: Vec<&SendMsg> =
            Self::filter_available_sends_in_view(g, rlab, sends, Some((&view, Some(spos))), false)
                .collect();
        if blockers.is_empty() {
            return true;
        }
        match timed {
            Some(cfg)
                if rlab.wait().is_some()
                    && rlab.comm() != CommunicationModel::NoOrder
                    && rlab.comm() != CommunicationModel::TotalOrder =>
            {
                // Evicted (overtaken) blockers are no blockers.
                let blockers: Vec<&SendMsg> = blockers
                    .into_iter()
                    .filter(|b| {
                        !Self::send_overtaken_in_view(
                            g,
                            b,
                            rlab.pos(),
                            Some((&view, Some(spos))),
                        )
                    })
                    .collect();
                if blockers.is_empty() {
                    return true;
                }
                let mut dcs = TimedDcs::build(g, cfg, Some(&view), Some(rlab.pos()));
                if dcs.base_feasible() {
                    blockers
                        .iter()
                        .all(|b| !dcs.probe_recv_rf(rlab.pos(), b.pos()))
                } else {
                    // Tripwire: treat blockers as real (never admits an
                    // execution the untimed rule would reject).
                    false
                }
            }
            _ => false,
        }
    }

    /// Inbox consistency for set semantics: order does not matter.
    /// True iff the chosen subset satisfies bounds and every send is valid/available.
    pub(crate) fn is_revisit_consistent_inbox(
        &self,
        g: &ExecutionGraph,
        inbox: &Inbox,
        sends: &[Event],
    ) -> bool {
        if let Some(max) = inbox.max() {
            if sends.len() > max {
                return false;
            }
        }
        if sends.len() < inbox.min() {
            return false;
        }

        // Each chosen send must exist, match, be undropped, and not already read by another receiver.
        for &s in sends {
            let Some(slab) = g.send_label(s) else {
                return false;
            };
            if slab.is_dropped() || !inbox.matches(slab) {
                return false;
            }
            if slab.reader().is_some_and(|r| r != inbox.pos()) {
                return false;
            }
        }
        true
    }

    pub(crate) fn normalize_event_set(events: &mut Vec<Event>) {
        // Canonicalize subset representation before comparisons/ownership checks.
        events.sort();
        events.dedup();
    }

    // the owner of a set of (send) events is the newer one (from which backward revisit are generated)
    pub(crate) fn inbox_owner(g: &ExecutionGraph, events: &[Event]) -> Option<Event> {
        events.iter().copied().max_by_key(|e| g.label(*e).stamp())
    }
}
