//! Online shop stock race: two order handlers can sell the same last item.
//!
//! ## What this example is about, in plain words
//!
//! Picture a small online shop. Shoppers browse, and now and then one of
//! them orders a product. The shop keeps a stock count for each product,
//! and the count must never go below zero: selling items you do not have
//! means promising goods that cannot be shipped.
//!
//! The shop is built from small separate programs that only talk to each
//! other by sending messages:
//!
//!   * a shopper, who places an order and then waits for the answer
//!     before ordering again;
//!   * an order handler, which takes one order at a time and sees it
//!     through;
//!   * a stock keeper, the only one allowed to look at or change the
//!     stock counts;
//!   * a shipping desk, which books the delivery;
//!   * a post office for messages (in the real system, a product called
//!     Kafka) that holds new orders until an order handler is free to
//!     pick one up.
//!
//! An order goes like this. The shopper posts "I want 2 of product A".
//! A free order handler picks it up and asks the stock keeper "are there
//! at least 2 of A?". The stock keeper answers yes or no. On no, the
//! handler tells the shopper "sorry". On yes, the handler tells the stock
//! keeper "then take 2 of A off the count", waits until that is done,
//! asks the shipping desk to book the delivery, waits for the booking,
//! and finally tells the shopper "your order is on its way".
//!
//! With one order handler this is safe: it finishes each order before it
//! looks at the next one. When the shop gets busy, the platform it runs
//! on starts a second copy of the order handler, and the post office
//! shares the orders between the two copies. Now this can happen: one
//! item of A is left; both handlers ask "is there at least 1 of A?" at
//! nearly the same moment; the stock keeper truthfully says yes to both;
//! then both say "take 1 off", and the count drops to minus one. The
//! trouble is that "is there enough?" and "take it off" are two separate
//! requests, and the other handler can slip its question in between.
//! The repaired version (v2) asks the stock keeper to do both in one go:
//! "if there are at least 2 of A, take them off and tell me yes,
//! otherwise tell me no". The first version (v1) keeps the two steps.
//!
//! The checker tries every order in which the messages can be handled
//! and every product and amount the shoppers can pick, and reports a
//! failure if the stock count can ever go below zero.
//!
//! Timers. The original design has no timers at all, and neither does
//! this example unless you ask for one:
//!
//!   * In timed mode every message takes some time to travel, somewhere
//!     between a shortest and a longest travel time, and a message that
//!     sits unread for too long after it arrives is thrown away. The
//!     program is exactly the same as in untimed mode; only the checker's
//!     view of time changes. The trip to the shipping desk and back is
//!     counted as travel time.
//!   * Optionally, a shopper gives up waiting for an answer after a set
//!     time (a checkout deadline). The checker then reports whether a
//!     shopper could ever be left waiting longer than that. The repaired
//!     version answers sooner, because it needs two fewer messages.
//!
//! Source: Ehsan Khamespanah and Mohammad Mahdi Jaghoori, "20 Years of
//! Actor Model Checking with Rebeca From Dining Philosophers to
//! Micro-services", in E. A. Lee, M. R. Mousavi and C. Talcott (eds.),
//! Rebeca for Actor Analysis in Action (Marjan Sirjani Festschrift),
//! Lecture Notes in Computer Science 15560, pp. 26-43, Springer, 2025,
//! doi 10.1007/978-3-031-85134-6_2 (Sections 2 and 4, Listings 2.1 to
//! 2.5 and 4.1 to 4.4, Table 1). The matching untimed Rebeca models are
//! OnlineRetailSystem.rebeca, OnlineRetailSystemKafkaV1.rebeca and
//! OnlineRetailSystemKafkaV2.rebeca from the Rebeca example library
//! (rebeca-lang.org, "Micro Service"). The second author is at Dockmeh
//! B.V., Amsterdam.
//!
//! ## How the model maps onto TraceForge
//!
//! Threads (default 5, all long-lived, harness Init from main as in
//! swim_timed.rs):
//!
//!   Customer c (x C)        Listing 4.2: R orders, one at a time; each
//!                           order picks a product in 0..P and an amount
//!                           in 1..=Q (the models' ?(0,1) and ?(1,2)),
//!                           then waits for the Reply on its own topic.
//!   OrderService k (x N)    Listing 4.3 (v1) or 4.4 (v2): getMessage is
//!                           a blocking receive for the next Order; while
//!                           an order is open it reads ONLY from the
//!                           inventory (and its own shipping message), so
//!                           queued orders wait, exactly like the Kafka
//!                           consumer that asks for the next message only
//!                           after answering.
//!   Inventory (x 1)         Listing 2.3 (v1) or 4.4 (v2); stock[p] = S.
//!
//! Message flow of one order (v1; v2 skips Confirm/Confirmed and takes
//! the items inside Check):
//!
//!   customer  --Order{p,q}-->              service      (Kafka order topic)
//!   service   --Check{p,q}-->              inventory
//!   inventory --Status{available}-->       service
//!   service   --Confirm{p,q}-->            inventory    (v1 only)
//!   inventory --Confirmed-->               service      (v1 only)
//!   service   --ShippingScheduled-->       service      (shipping round trip)
//!   service   --Reply{success}-->          customer     (Kafka customer topic)
//!
//! Receives in TraceForge do NOT filter by Rust type (a mismatching type
//! panics), so every thread has exactly one message enum and all
//! filtering is by sender: services read orders with `sender != inventory
//! && sender != self` and order replies with `sender == inventory`.
//!
//! `--variant direct` is the Section 2 model (Listings 2.1 to 2.5, no
//! broker): the order service is purely reactive and keeps several
//! orders open at once, with the order data carried in every message
//! (the listing's `cu, product, qty` parameters). The inventory echoes
//! `customer, product, qty` in all variants; the Kafka services ignore
//! the echo, so this changes no behaviour.
//!
//! Harness messages, not the paper: OsMsg::Init / CustMsg::Init (thread
//! ids) and OsMsg::Done / InvMsg::Done (a customer releases its
//! service(s) after R orders, a service releases the inventory), so the
//! reactive threads terminate in a bounded model.
//!
//! ## Deviations from the paper (deliberate)
//!
//!   * No broker thread. Customer to service and service to customer are
//!     direct channels. Each Rebeca reply topic has exactly one consumer,
//!     so it is just an ordered channel. The paper's broker dispatches
//!     the order topic WITHOUT a partitioning key ("every order is sent
//!     only once and therefore we can abstract away the concept of the
//!     partitioning key. So Kafka is free to deliver the messages to any
//!     of the subscribers"); `--assign any` is that paper-faithful
//!     dispatch (every order picks a service freely; it also lets an
//!     order queue at a busy service while the other is idle, an
//!     over-approximation of the broker). The default `--assign fixed`
//!     (customer c always uses service c mod N) is this harness's cost
//!     choice, not the paper's: it does not hide the race, because the
//!     race needs two orders open at different services at once, and a
//!     customer has only one open order, so those two orders always come
//!     from different customers (measured: v1 FIREs under both
//!     assignments). Transit [L,U] is read as the whole
//!     producer-to-consumer trip through the broker.
//!   * The listing's buffer code is not copied. Listing 4.1 lines 40-44
//!     (and both local models) shift the topic buffer using `size`
//!     instead of `cnt`, so the head slot is never overwritten after a
//!     consume: with 2 queued messages the first is delivered twice and
//!     the second is lost. This contradicts the paper's own "exactly-once"
//!     and "first-in-first-out" statements; we implement the stated
//!     intent (every order delivered once, in order).
//!   * ShippingService is not a thread. Its arrangeDelivery /
//!     shippingIsScheduled round trip is a single self-addressed
//!     ShippingScheduled message sent with send_msg_timed over
//!     [2L, 2U + sd] (outbound hop, the desk's read slack sd, return
//!     hop; the service's own read slack is added by the checker at the
//!     read). The service stays busy until it reads it, as in Listing 4.3
//!     where getMessage follows shippingIsScheduled. In untimed mode it is
//!     an ordinary self-message (send_msg_timed is inert by library
//!     design), so both modes verify the same program. Real shipping
//!     services take time to book a delivery; this keeps that cost.
//!   * Browsing without buying (`buying = ?(true,false)` then
//!     `self.browse()`) is omitted: every round places an order, and the
//!     infinite browse loop is bounded by `--rounds R`. A non-buying
//!     browse sends nothing and changes no shared state, so it can only
//!     remove orders.
//!   * Two products with 5 items each and amounts 1..=2 are the paper's
//!     setting (`--products 2 --stock 5 --max-qty 2`); the default is one
//!     product for cost (products are independent counters, the race is
//!     per product).
//!   * Timed mode only: sd (message storage lifetime past arrival) plays
//!     the role of broker retention. Real Kafka keeps messages for days
//!     (default log.retention.hours = 168); TraceForge makes a message
//!     unreadable sd after it arrives. In configurations where an order
//!     can wait for a busy Kafka service (`--order-services 1`,
//!     `--assign any`, or more customers than services), a small sd
//!     silently discards real executions as blocked (measured below: at
//!     L = U = 1, execs = 0 at sd = 5). Raise `--sd-ratio` until timed execs match
//!     what the timing allows; the binary prints a NOTE in these
//!     configurations.
//!   * Optional `--deadline-ratio`: a checkout request timeout at the
//!     customer, a bounded reading of the local .property files'
//!     untimed formula G(!c1Buying || F(!c1Buying)). It is not in the
//!     paper (which claims deadlock freedom instead), is off by default,
//!     and a real storefront does have such a timeout.
//!   * Not modelled, because neither the paper nor the models have them:
//!     reservation expiry (v2 takes the items immediately, there is no
//!     reservation), consumer rebalancing or redelivery timers,
//!     at-least-once duplicates, crashes, message loss.
//!
//! ## Timeout-validation assumptions (verification harness, NOT the protocol)
//!
//! Only relevant with `--deadline-ratio`; without it the model has no
//! finite wait and no timeout branch at all. The timeout branch
//! (rf = None) of a finite-wait receive is by design always explorable,
//! in either mode: it only means "the reply had not arrived by the
//! deadline IN THIS BRANCH". An assertion placed directly on it would
//! fire at any parameters. assume_reply_was_late therefore follows the
//! timeout with a BLOCKING timed receive for the very reply the customer
//! was waiting for (the customer has exactly one open order, so the next
//! message from any service is that reply). The read is feasible exactly
//! when the reply could genuinely be read at or after the deadline, so
//! spurious timeouts become blocked (discarded) executions instead of
//! false counterexamples. A real storefront does not re-read a reply
//! after giving up; this is checker scaffolding kept out of the protocol
//! logic. Costs: deadline holds report blocked > 0 by design, and an
//! untimed checker still FIREs at every deadline (it cannot bound time).
//!
//! ## Properties checked
//!
//!   SafeStockLevel (paper Section 4.3, both .property files):
//!       stock[p] >= 0 for every product p. Checked at the inventory
//!       right after every decrement with traceforge::assert. Stock is
//!       local state of one thread, so the check needs no clock and no
//!       global observer.
//!   Bounded response (optional, deviation above): a customer's Reply is
//!       read strictly before D has passed since it sent its Order (a
//!       reply that arrives exactly at the deadline may lose to the
//!       timeout, so it counts as late), checked with the
//!       timeout-validation idiom.
//!   Deadlock freedom (paper: "no deadlock"): without a deadline every
//!       receive is blocking, so blocked > 0 in UNTIMED mode is a genuine
//!       deadlock: the binary prints a DEADLOCK line and exits 3 (distinct
//!       from a hold's 0 and a violation's 101). This is enforced only in
//!       untimed runs without a deadline. In timed mode blocked > 0 is
//!       expected even without a deadline: TraceForge's timed receive may
//!       take a message that arrived later than another unread matching
//!       one, and the skipped message is then dropped (the engine's
//!       skip-means-evict rule), so that execution ends blocked; with
//!       queueing, sd evictions add more (see deviations). None of these
//!       is a behaviour of the paper's exactly-once broker. What counts
//!       is that timed mode keeps the completed executions (execs) the
//!       timing allows.
//!
//! ## Evidence counters and vacuity warnings
//!
//! Static counters, reset before each verify, accumulated over all
//! explored executions (revisited prefixes may re-count: treat them as
//! zero/nonzero evidence):
//!
//!   accepted / rejected   Status replies with available = true / false.
//!                         rejected > 0 proves stock exhaustion was
//!                         reached, the only place an oversell can occur.
//!   overlap               the inventory handled a Check while another
//!                         order was open at some service (a global
//!                         in-flight count, evidence only, never read by
//!                         protocol logic).
//!   race_window           (v1, direct) the inventory answered a Check
//!                         while another order of the same product had
//!                         been approved but not yet taken off the count:
//!                         the check-then-act gap was open. Local to the
//!                         inventory thread.
//!   contended_reject      a Check was rejected while another order was
//!                         open: in v2 this is the would-be oversell that
//!                         the atomic step turned into a refusal.
//!   oversold              a count went below zero (counted before the
//!                         assert, so --keep-going reports how often).
//!   in_time / late        deadline replies read in time / validated late.
//!
//! The binary prints a WARNING when a run cannot count as a hold:
//! execs = 0; rejected = 0 (stock never ran out); overlap = 0 with two or
//! more services or in direct mode (orders never met at the inventory);
//! contended_reject = 0 for v2 with two or more services; a deadline run
//! with in_time = 0. The rejected, overlap and contended_reject warnings
//! are evidence for the stock property only, so a deadline run skips them
//! (its cells use a large stock on purpose, so no order is ever rejected)
//! and prints a NOTE instead: a deadline verdict is judged by execs > 0,
//! in_time > 0 and late = 0. It prints a NOTE for a single Kafka service
//! (overlap is 0 by construction) and for timed runs where orders can
//! queue (sd acts as retention, compare execs with baseline); for
//! blocked > 0 without a deadline it prints a NOTE in timed mode and a
//! DEADLOCK line (exit 3) in untimed mode. With --keep-going a VIOLATIONS
//! line replaces the hold warnings when oversold > 0 or late > 0.
//!
//! ## CLI parameters (ratios are over U, like swim_timed)
//!
//!   --mode baseline|timed|compare   verification mode (default timed;
//!                                   compare's baseline leg aborts with
//!                                   exit 101 whenever baseline FIREs)
//!   --variant v1|v2|direct          v1 = Kafka + non-atomic check then
//!                                   take (Listing 4.3), v2 = Kafka +
//!                                   atomic check-and-take (Listing 4.4),
//!                                   direct = Section 2, no broker,
//!                                   reactive service, non-atomic
//!                                   (default v1)
//!   --order-services N              service instances, 1..=4 (default 2
//!                                   for v1/v2, 1 for direct as in
//!                                   Listing 2.5; 1 = before auto-scaling)
//!   --assign fixed|any              customer c uses service c mod N, or
//!                                   any service per order (default fixed)
//!   --customers C                   default 2 (the paper's Table 1)
//!   --rounds R                      orders per customer (default 2)
//!   --stock S                       initial count per product (default 5)
//!   --products P                    default 1 (paper: 2)
//!   --max-qty Q                     amounts 1..=Q (default 2, paper)
//!   --u U                           global transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U) (default 0.0)
//!   --sd-ratio SR                   sd = round(SR * U), storage lifetime
//!                                   past arrival (default 0.0)
//!   --deadline-ratio DR             D = round(DR * U); absent = no
//!                                   deadline, blocking reply wait (default)
//!   --keep-going                    explore past a violation, report counts
//!
//! Exit codes: 0 = no violation over the explored state space; 101 = a
//! property violation (stock below zero, or a late reply); 2 = CLI misuse
//! (including a negative or non-finite ratio), so exit-code-based sweep
//! harnesses cannot mistake a typo for a FIRE; 3 = a genuine deadlock
//! (blocked > 0 in an untimed run without a deadline), so a deadlock is
//! never recorded as a hold.
//!
//! ## Expected verdicts (matrix-verified)
//!
//! Every line below was run (single-threaded, `timeout 120`); none is a
//! prediction. "execs a = b" compares the two modes or two runs named.
//! Default cell = C=2 R=2 S=5 P=1 Q=2 fixed assignment.
//!
//! SafeStockLevel (the paper's property):
//!
//!   v1, 2 services: FIRE (exit 101) in BOTH modes, at every timing
//!       tried: baseline at the minimal cell (R=1 S=1 Q=1), the default
//!       cell, P=2, and C=3 R=1 S=3; timed at the minimal cell (L=0 U=1
//!       sd=0; L=U=2) and the default cell at (U,L,sd) = (1,0,0),
//!       (1,1,0), (2,0,1), (2,2,0), (2,1,1), and P=2. Every counter-
//!       example read is the paper's race: the inventory answers both
//!       services' Checks with available = true before either Confirm
//!       (default cell: 1 + 1 leaves 3, then two concurrent orders of 2
//!       both pass 3 >= 2 and the count ends at -1); the timed witness
//!       at L=U=2 reads both Checks at 4 and both Confirms at 8.
//!       --keep-going, minimal cell: execs 6 and oversold 8 in both modes.
//!       --assign any, minimal cell: FIRE baseline and timed (L=0, and
//!       L=U=1), witness with the two customers on different services.
//!   v1, 2 services, R=1 S=5 (total demand 4 <= 5): HOLD, execs 80 in
//!       both modes, race_window > 0 but rejected = 0 (WARNING printed):
//!       a control showing the race window alone is not a violation.
//!   1 service (before auto-scaling), v1 and v2: HOLD in both modes, as
//!       in Table 1's 1O rows. Default cell execs 320 baseline = 320 timed
//!       (L=0 U=1 sd=0), rejected > 0, overlap = 0 by construction; v1
//!       P=2: 5120 = 5120 (timed 26 s); v1 C=3 R=1 S=3: 720 = 720. Orders
//!       queue here, so sd matters in timed mode: v1 default cell at
//!       L=U=1 gives execs 0 at sd in {0, 4, 5} (vacuous, WARNING), 164 at
//!       sd=6, 192 at sd in {7, 8, 10, 12}, 320 = baseline at sd in {20, 40}.
//!   v2, 2 services: HOLD in both modes at every cell tried, with
//!       rejected, overlap and contended_reject all > 0:
//!         minimal cell      baseline execs 6 (contended 3), timed 6 (6)
//!         default cell      baseline 320 (rejected 240, overlap 648,
//!                           contended 57); timed 320 at (U,L,sd) =
//!                           (1,0,0) and (2,0,1); 88 at L=U in {1, 2};
//!                           192 at (2,1,1); contended >= 51 in each
//!         P=2               5120 in both modes (timed 14.6 s)
//!         C=3 R=1 S=3       480 in both modes
//!         --assign any      minimal cell baseline 96; timed L=0: 96;
//!                           L=U=1: 8 at sd=0, 32 at sd in {1, 2}, 64 at
//!                           sd=4, 96 at sd in {6, 20} (queueing NOTE)
//!   direct (Section 2, 1 reactive service, 2 customers): FIRE in BOTH
//!       modes at the default cell and the minimal cell, and in baseline
//!       at P=2. Witness (timed, L=1 U=2): the single service forwards
//!       both customers' Checks before either Confirm, the same race
//!       inside one service. Control R=1 S=5: HOLD, execs 296 in both
//!       modes. --keep-going baseline default cell: execs 42498, oversold
//!       542. See the note on Table 1 below.
//!
//!   No timing boundary exists for SafeStockLevel: nothing in the
//!   program separates two services' Checks in time, so the race is
//!   realizable at every L, U, sd, including the lockstep L = U.
//!
//! Deadlock freedom: every untimed hold above without a deadline has
//! blocked = 0 (v1 and v2 with 1 service at the default cell; v1 with
//! 1 service at P=2 and at C=3 R=1 S=3; v2 with 2 services at the minimal and default cells, P=2,
//! C=3 R=1 S=3 and --assign any; the v1 and direct R=1 S=5 controls), so
//! none prints DEADLOCK and all exit 0.
//!
//! Bounded response (--deadline-ratio; S=20 so no order is rejected or
//! oversold; fixed assignment, 2 services, direct with 1 service):
//!
//!   baseline: FIRE at every deadline (tried v2, D=50): an untimed
//!       checker cannot bound response time.
//!   timed v1:     FIRE iff D <= 8(U + sd), HOLD from 8(U + sd) + 1
//!   timed v2:     FIRE iff D <= 6(U + sd), HOLD from 6(U + sd) + 1
//!   timed direct: FIRE iff D <= 8(U + sd), HOLD from 8(U + sd) + 1
//!
//!   The count is the hops of an accepted order, each at most U + sd:
//!   Order, Check, Status, [Confirm, Confirmed in v1 and direct], the two
//!   shipping hops, Reply. So the atomic fix also shortens the worst
//!   response by 2(U + sd). The FIRE at D equal to the worst response
//!   shows that a reply read exactly at the deadline counts as late (see
//!   Properties checked). Both sides were run, and every FIRE was a
//!   certified timed counterexample (the witness read in full, v2 at U=1
//!   D=6, has the Order read at 1, Check at 2, Status at 3, the shipping
//!   message at 5 and the late Reply read at 6, every hop at its bound):
//!     v1, v2 at R=1: (U,sd,L) = (1,0,0), (1,1,0), (2,0,0), (2,1,0),
//!         (1,0,1), (2,0,2), (2,0,1), (2,1,1), (3,2,1)
//!     v1, v2 at R=2: (1,0,0), (2,1,0), (2,0,2); v2 also (2,1,1)
//!     direct at R=1: (1,0,0), (2,1,1)
//!   The boundary is independent of L (the timer starts at the
//!   customer's own Order send and L never raises an upper bound) and of
//!   R (with fixed assignment no order waits for a busy service). Holds
//!   keep every completed execution of the same cell without a deadline:
//!   execs 4032 = 4032 (v1 R=2), 320 = 320 (v2 R=2), 64 = 64 (v1 R=1,
//!   L=1 U=2 sd=1), 128 = 128 (v2 R=2, L=U=2), 296 = 296 and 152 = 152
//!   (direct R=1 at (1,0,0) and (2,1,1)); in_time > 0 and late = 0 on
//!   every hold, blocked > 0 by design. Every deadline hold prints no
//!   WARNING and one NOTE saying the stock-race evidence checks are
//!   skipped (S=20 rules out rejections on purpose). Not measured:
//!   deadlines where orders queue (1 service, --assign any, C > N).
//!
//! Wall time (release build, one core): default v1 timed FIRE 0.4 s,
//! default v2 timed hold 0.9 s, v1 R=2 deadline hold 27 s.
//!
//! ## Note on Table 1's "Without Kafka" rows
//!
//! Table 1 reports the Section 2 model (no broker, 1 order service) as
//! correct for 2 and 3 customers, and the paper names the property: the
//! introduction to Section 4 says of the Section 2 model "Model checking
//! with Afra proves that there is no deadlock in this model and
//! additionally we verify that the number of available items never falls
//! below zero (see Sect. 4.3)". Our encoding of Listings 2.1 to 2.5
//! (`--variant direct`) breaks exactly that claim: it FIREs at the
//! paper's own values (S=5, Q=2) with a certified counterexample. The
//! Listing 2.2 OrderService is purely reactive, so it can forward a
//! second customer's checkAvailability before the first order's
//! confirmBoughtItem, which is exactly the Kafka 2O race. The authors'
//! Afra model may differ from the printed listings (for example in queue
//! size bounds; the local OnlineRetailSystem.rebeca has no .property
//! file), so this should be checked against their model before calling
//! the paper wrong.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Nondet, Stats, WaitTime};

// Evidence counters (see the doc header): reset before each verify,
// read after. Never read by protocol logic.
static ACCEPTED: AtomicUsize = AtomicUsize::new(0);
static REJECTED: AtomicUsize = AtomicUsize::new(0);
static OVERLAP: AtomicUsize = AtomicUsize::new(0);
static RACE_WINDOW: AtomicUsize = AtomicUsize::new(0);
static CONTENDED_REJECT: AtomicUsize = AtomicUsize::new(0);
static OVERSOLD: AtomicUsize = AtomicUsize::new(0);
static REPLIES_IN_TIME: AtomicUsize = AtomicUsize::new(0);
static LATE_REPLIES: AtomicUsize = AtomicUsize::new(0);

// Orders currently open at some service (read an Order, not yet sent
// the Reply). Reset at the start of every execution inside the verify
// closure. Evidence only: the inventory reads it to bump OVERLAP, and
// no protocol decision ever depends on it.
static IN_FLIGHT_ORDERS: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_ORDER_SERVICES: usize = 2;
const DIRECT_ORDER_SERVICES: usize = 1;
const DEFAULT_CUSTOMERS: usize = 2;
const DEFAULT_ROUNDS: u32 = 2;
const DEFAULT_STOCK: i64 = 5;
const DEFAULT_PRODUCTS: usize = 1;
const DEFAULT_MAX_QTY: usize = 2;
const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 0.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const MAX_ORDER_SERVICES: usize = 4;

/// Everything an order service receives.
#[derive(Clone, Debug, PartialEq)]
enum OsMsg {
    /// Harness: the inventory's thread id.
    Init { inventory: ThreadId },
    /// Kafka order topic (Listing 4.2 line 14) or placeOrder (Listing 2.1).
    Order { customer: ThreadId, product: usize, qty: i64 },
    /// Harness: this customer places no more orders.
    Done,
    /// availabilityStatus (Listings 2.3 / 4.3 / 4.4).
    Status { customer: ThreadId, available: bool, product: usize, qty: i64 },
    /// confirm (v1 and direct only).
    Confirmed { customer: ThreadId, product: usize, qty: i64 },
    /// The folded arrangeDelivery / shippingIsScheduled round trip,
    /// self-addressed (see deviations).
    ShippingScheduled { customer: ThreadId },
}

/// Everything the inventory receives.
#[derive(Clone, Debug, PartialEq)]
enum InvMsg {
    /// checkAvailability. `from` is Rebeca's `sender`.
    Check { from: ThreadId, customer: ThreadId, product: usize, qty: i64 },
    /// confirmBoughtItem (v1 and direct only).
    Confirm { from: ThreadId, customer: ThreadId, product: usize, qty: i64 },
    /// Harness: this service is finished.
    Done,
}

/// Everything a customer receives.
#[derive(Clone, Debug, PartialEq)]
enum CustMsg {
    /// Harness: the order services' thread ids.
    Init { services: Vec<ThreadId> },
    /// Kafka customer topic (1 or -1 in the listing) / delivered(success).
    Reply { success: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    /// Kafka, check then take as two requests (Listing 4.3).
    V1,
    /// Kafka, atomic check-and-take (Listing 4.4).
    V2,
    /// No broker, reactive order service, check then take (Section 2).
    Direct,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Variant::V1 => "v1",
            Variant::V2 => "v2",
            Variant::Direct => "direct",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Assign {
    Fixed,
    Any,
}

impl Assign {
    fn name(self) -> &'static str {
        match self {
            Assign::Fixed => "fixed",
            Assign::Any => "any",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    l: u64,
    sd: u64,
    deadline: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct Setup {
    variant: Variant,
    order_services: usize,
    assign: Assign,
    customers: usize,
    rounds: u32,
    stock: i64,
    products: usize,
    max_qty: usize,
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

impl Bounds {
    fn from_ratios(u: u64, l_ratio: f64, sd_ratio: f64, deadline_ratio: Option<f64>) -> Self {
        if u < 1 {
            cli_bail("U must be >= 1");
        }
        // A negative or NaN ratio would otherwise saturate to 0 in the
        // float-to-integer cast below and run silently at a wrong bound.
        let ratios = [
            ("--l-ratio", Some(l_ratio)),
            ("--sd-ratio", Some(sd_ratio)),
            ("--deadline-ratio", deadline_ratio),
        ];
        for (flag, r) in ratios.into_iter().filter_map(|(f, r)| r.map(|r| (f, r))) {
            if !r.is_finite() || r < 0.0 {
                cli_bail(&format!("{flag} must be a finite number >= 0, got {r}"));
            }
        }
        let scale = |r: f64| (r * u as f64).round() as u64;
        let l = scale(l_ratio);
        let sd = scale(sd_ratio);
        if l > u {
            cli_bail("transit lower bound L must be <= U (check --l-ratio)");
        }
        let deadline = deadline_ratio.map(scale);
        if deadline == Some(0) {
            cli_bail("deadline must round to >= 1 (check --deadline-ratio)");
        }
        Self { u, l, sd, deadline }
    }
}

/// Transit window of the folded shipping round trip, as seen by the
/// service's read: arrangeDelivery hop [L, U], the shipping desk's read
/// slack sd, shippingIsScheduled hop [L, U]. The service's own read
/// slack sd is added by the checker. Inert in untimed mode.
fn shipping_round_trip(b: Bounds) -> (u64, u64) {
    (2 * b.l, 2 * b.u + b.sd)
}

// =====================================================================
// Timeout-validation assumption (verification harness, NOT the
// protocol; see the doc header). Only used with --deadline-ratio.
// =====================================================================

/// Assume this order's Reply was genuinely late: block until it is
/// readable at or after the current (post-deadline) local time. Timed
/// infeasible, so the execution blocks and is discarded, when the Reply
/// provably arrived inside the deadline. The customer has exactly one
/// open order, so the next message from any service is that Reply.
fn assume_reply_was_late(main_tid: ThreadId) {
    match traceforge::recv_tagged_msg_block_timed::<_, CustMsg>(move |s, _tag| s != main_tid) {
        CustMsg::Reply { .. } => {}
        // Unreachable: main sends only Init, services send only Reply.
        m => panic!("customer: unexpected {m:?} in validation read"),
    }
}

// =====================================================================
// Customer (Listings 2.1 and 4.2)
// =====================================================================

fn customer(index: usize, main_tid: ThreadId, s: Setup, b: Bounds) {
    let services = match traceforge::recv_tagged_msg_block_timed::<_, CustMsg>(move |snd, _| {
        snd == main_tid
    }) {
        CustMsg::Init { services } => services,
        m => panic!("customer: expected Init, got {m:?}"),
    };
    let me = thread::current().id();
    let n = services.len();

    for _round in 0..s.rounds {
        // browse with buying = true: ?(0,1) product, ?(1,2) amount.
        let product = if s.products > 1 { (0..s.products).nondet() } else { 0 };
        let qty = if s.max_qty > 1 { (1..=s.max_qty).nondet() } else { 1 } as i64;
        let target = match s.assign {
            Assign::Fixed => services[index % n],
            Assign::Any if n > 1 => services[(0..n).nondet()],
            Assign::Any => services[0],
        };
        traceforge::send_msg(target, OsMsg::Order { customer: me, product, qty });

        // Wait for the answer before browsing again ("a customer is
        // allowed to continue browsing only after finishing processing
        // of its previous request").
        match b.deadline {
            None => {
                match traceforge::recv_tagged_msg_block_timed::<_, CustMsg>(move |snd, _| {
                    snd != main_tid
                }) {
                    CustMsg::Reply { .. } => {}
                    m => panic!("customer: expected Reply, got {m:?}"),
                }
            }
            Some(d) => {
                match traceforge::recv_tagged_msg_timed::<_, CustMsg>(
                    move |snd, _| snd != main_tid,
                    WaitTime::Finite(d),
                ) {
                    Some(CustMsg::Reply { .. }) => {
                        REPLIES_IN_TIME.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(m) => panic!("customer: expected Reply, got {m:?}"),
                    None => {
                        // Harness assumption (doc header): keep only
                        // executions where the reply really was late.
                        assume_reply_was_late(main_tid);
                        LATE_REPLIES.fetch_add(1, Ordering::Relaxed);
                        // Bounded response violated.
                        traceforge::assert(false);
                        return;
                    }
                }
            }
        }
    }

    // Harness: release the service(s) this customer may have used.
    match s.assign {
        Assign::Fixed => traceforge::send_msg(services[index % n], OsMsg::Done),
        Assign::Any => {
            for sv in &services {
                traceforge::send_msg(*sv, OsMsg::Done);
            }
        }
    }
}

// =====================================================================
// Order service, Kafka variants (Listings 4.3 and 4.4)
// =====================================================================

fn read_inventory_reply(inv: ThreadId) -> OsMsg {
    traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| snd == inv)
}

fn kafka_order_service(main_tid: ThreadId, variant: Variant, expected_done: usize, b: Bounds) {
    let inv = match traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| snd == main_tid) {
        OsMsg::Init { inventory } => inventory,
        m => panic!("service: expected Init, got {m:?}"),
    };
    let me = thread::current().id();
    let mut done = 0;

    while done < expected_done {
        // getMessage(ORDER_TOPIC): the next order, or a customer's Done.
        match traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| {
            snd != inv && snd != me
        }) {
            OsMsg::Order { customer, product, qty } => {
                IN_FLIGHT_ORDERS.fetch_add(1, Ordering::Relaxed);
                // receive(): inventory.checkAvailability(product, qty)
                traceforge::send_msg(inv, InvMsg::Check { from: me, customer, product, qty });
                let available = match read_inventory_reply(inv) {
                    OsMsg::Status { available, .. } => available,
                    m => panic!("service: expected Status, got {m:?}"),
                };
                if available && variant == Variant::V1 {
                    // availabilityStatus(true) in v1: confirmBoughtItem,
                    // then wait for confirm().
                    traceforge::send_msg(inv, InvMsg::Confirm { from: me, customer, product, qty });
                    match read_inventory_reply(inv) {
                        OsMsg::Confirmed { .. } => {}
                        m => panic!("service: expected Confirmed, got {m:?}"),
                    }
                }
                if available {
                    // arrangeDelivery / shippingIsScheduled, folded.
                    let (l, u) = shipping_round_trip(b);
                    traceforge::send_msg_timed(me, OsMsg::ShippingScheduled { customer }, l, u);
                    match traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| {
                        snd == me
                    }) {
                        OsMsg::ShippingScheduled { .. } => {}
                        m => panic!("service: expected ShippingScheduled, got {m:?}"),
                    }
                }
                // kafka.sendMessage(customerTopic, 1 or -1), then
                // getMessage(ORDER_TOPIC) at the top of the loop.
                traceforge::send_msg(customer, CustMsg::Reply { success: available });
                IN_FLIGHT_ORDERS.fetch_sub(1, Ordering::Relaxed);
            }
            OsMsg::Done => done += 1,
            m => panic!("service: unexpected {m:?}"),
        }
    }
    traceforge::send_msg(inv, InvMsg::Done);
}

// =====================================================================
// Order service, no broker (Listing 2.2): reacts to each message as it
// arrives and may have several orders open at once.
// =====================================================================

fn direct_order_service(main_tid: ThreadId, expected_done: usize, b: Bounds) {
    let inv = match traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| snd == main_tid) {
        OsMsg::Init { inventory } => inventory,
        m => panic!("service: expected Init, got {m:?}"),
    };
    let me = thread::current().id();
    let mut done = 0;

    // A customer sends Done only after the reply to its last order, so
    // once every Done is read no order of this service is still open.
    while done < expected_done {
        match traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| snd != main_tid) {
            OsMsg::Order { customer, product, qty } => {
                // placeOrder
                IN_FLIGHT_ORDERS.fetch_add(1, Ordering::Relaxed);
                traceforge::send_msg(inv, InvMsg::Check { from: me, customer, product, qty });
            }
            OsMsg::Status { customer, available, product, qty } => {
                // availabilityStatus
                if available {
                    traceforge::send_msg(inv, InvMsg::Confirm { from: me, customer, product, qty });
                } else {
                    traceforge::send_msg(customer, CustMsg::Reply { success: false });
                    IN_FLIGHT_ORDERS.fetch_sub(1, Ordering::Relaxed);
                }
            }
            OsMsg::Confirmed { customer, .. } => {
                // confirm: shipping.arrangeDelivery, folded.
                let (l, u) = shipping_round_trip(b);
                traceforge::send_msg_timed(me, OsMsg::ShippingScheduled { customer }, l, u);
            }
            OsMsg::ShippingScheduled { customer } => {
                // shippingIsScheduled: cu.delivered(true)
                traceforge::send_msg(customer, CustMsg::Reply { success: true });
                IN_FLIGHT_ORDERS.fetch_sub(1, Ordering::Relaxed);
            }
            OsMsg::Done => done += 1,
            m => panic!("service: unexpected {m:?}"),
        }
    }
    traceforge::send_msg(inv, InvMsg::Done);
}

// =====================================================================
// Inventory (Listings 2.3 and 4.4)
// =====================================================================

/// stockLevel[product] -= qty, followed by the SafeStockLevel check.
fn take_items(stock: &mut [i64], product: usize, qty: i64) {
    stock[product] -= qty;
    if stock[product] < 0 {
        OVERSOLD.fetch_add(1, Ordering::Relaxed);
    }
    // SafeStockLevel: stockLevel[p] >= 0.
    traceforge::assert(stock[product] >= 0);
}

fn inventory(s: Setup) {
    let atomic = s.variant == Variant::V2;
    let mut stock = vec![s.stock; s.products];
    // Evidence only: approvals given (Status true) whose Confirm has not
    // been handled yet, per product. Always 0 in v2.
    let mut approved_not_taken = vec![0usize; s.products];
    let mut done = 0;

    while done < s.order_services {
        match traceforge::recv_msg_block_timed::<InvMsg>() {
            InvMsg::Check { from, customer, product, qty } => {
                let others_open = IN_FLIGHT_ORDERS.load(Ordering::Relaxed) >= 2;
                if others_open {
                    OVERLAP.fetch_add(1, Ordering::Relaxed);
                }
                if approved_not_taken[product] > 0 {
                    RACE_WINDOW.fetch_add(1, Ordering::Relaxed);
                }
                let available = stock[product] >= qty;
                if available {
                    ACCEPTED.fetch_add(1, Ordering::Relaxed);
                    if atomic {
                        // v2: check and take in one message server.
                        take_items(&mut stock, product, qty);
                    } else {
                        approved_not_taken[product] += 1;
                    }
                } else {
                    REJECTED.fetch_add(1, Ordering::Relaxed);
                    if others_open {
                        CONTENDED_REJECT.fetch_add(1, Ordering::Relaxed);
                    }
                }
                traceforge::send_msg(from, OsMsg::Status { customer, available, product, qty });
            }
            InvMsg::Confirm { from, customer, product, qty } => {
                // confirmBoughtItem (v1, direct). Unreachable in v2.
                approved_not_taken[product] = approved_not_taken[product].saturating_sub(1);
                take_items(&mut stock, product, qty);
                traceforge::send_msg(from, OsMsg::Confirmed { customer, product, qty });
            }
            InvMsg::Done => done += 1,
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, b: Bounds, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_progress_report(usize::MAX);
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
    }
}

/// How many customers can send orders (and hence a Done) to service k.
fn expected_dones(k: usize, s: Setup) -> usize {
    match s.assign {
        Assign::Fixed => (0..s.customers).filter(|c| c % s.order_services == k).count(),
        Assign::Any => s.customers,
    }
}

fn reset_counters() {
    for c in [
        &ACCEPTED,
        &REJECTED,
        &OVERLAP,
        &RACE_WINDOW,
        &CONTENDED_REJECT,
        &OVERSOLD,
        &REPLIES_IN_TIME,
        &LATE_REPLIES,
        &IN_FLIGHT_ORDERS,
    ] {
        c.store(0, Ordering::Relaxed);
    }
}

fn run(mode: Mode, s: Setup, b: Bounds, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, b, keep_going);
    reset_counters();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        IN_FLIGHT_ORDERS.store(0, Ordering::Relaxed);
        let main_tid = thread::current().id();
        let inv = thread::spawn(move || inventory(s));
        let inv_id = inv.thread().id();
        let mut services = Vec::new();
        for k in 0..s.order_services {
            let expected = expected_dones(k, s);
            services.push(thread::spawn(move || match s.variant {
                Variant::Direct => direct_order_service(main_tid, expected, b),
                v => kafka_order_service(main_tid, v, expected, b),
            }));
        }
        let mut customers = Vec::new();
        for c in 0..s.customers {
            customers.push(thread::spawn(move || customer(c, main_tid, s, b)));
        }
        let service_ids: Vec<ThreadId> = services.iter().map(|h| h.thread().id()).collect();
        // Send every Init before joining anyone.
        for id in &service_ids {
            traceforge::send_msg(*id, OsMsg::Init { inventory: inv_id });
        }
        for h in &customers {
            traceforge::send_msg(h.thread().id(), CustMsg::Init { services: service_ids.clone() });
        }
        for h in customers {
            let _ = h.join();
        }
        for h in services {
            let _ = h.join();
        }
        let _ = inv.join();
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    accepted: usize,
    rejected: usize,
    overlap: usize,
    race_window: usize,
    contended_reject: usize,
    oversold: usize,
    in_time: usize,
    late: usize,
}

fn read_counts() -> Counts {
    Counts {
        accepted: ACCEPTED.load(Ordering::Relaxed),
        rejected: REJECTED.load(Ordering::Relaxed),
        overlap: OVERLAP.load(Ordering::Relaxed),
        race_window: RACE_WINDOW.load(Ordering::Relaxed),
        contended_reject: CONTENDED_REJECT.load(Ordering::Relaxed),
        oversold: OVERSOLD.load(Ordering::Relaxed),
        in_time: REPLIES_IN_TIME.load(Ordering::Relaxed),
        late: LATE_REPLIES.load(Ordering::Relaxed),
    }
}

/// True when an order can sit unread while its Kafka order service is
/// busy with another order (see the sd deviation in the doc header).
fn orders_can_queue(s: Setup) -> bool {
    s.variant != Variant::Direct
        && match s.assign {
            Assign::Fixed => s.customers > s.order_services,
            Assign::Any => s.customers >= 2,
        }
}

/// Vacuity guards (see the doc header). None of the WARNING outcomes
/// counts as a hold of the property it names. Returns true when the run
/// is a genuine deadlock (untimed, no deadline, blocked > 0, no
/// violation); main then exits 3 so a deadlock is never read as a hold.
fn warn_if_vacuous(label: &str, s: Setup, b: Bounds, mode: Mode, execs: usize, blocked: usize, c: Counts) -> bool {
    // Only reachable with --keep-going: without it a violation exits 101.
    let violated = c.oversold > 0 || c.late > 0;
    if violated {
        println!(
            "VIOLATIONS ({label}, --keep-going): oversold={} late={}: this run is a FIRE, not \
             a hold; blocked also counts the executions stopped at a violation.",
            c.oversold, c.late
        );
    }
    if execs == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}): this run verified \
             nothing; treat it as no data, not as a hold."
        );
    }
    if !violated {
        // Stock-race evidence. A deadline run judges bounded response and
        // uses a large stock on purpose, so these would flag every
        // deadline hold; they are skipped there (see the doc header).
        let stock_evidence = b.deadline.is_none();
        if !stock_evidence {
            println!(
                "NOTE ({label}): deadline run: the verdict is about bounded response, judged by \
                 execs > 0, in_time > 0 and late = 0; the stock-race evidence checks (rejected, \
                 overlap, contended_reject) are skipped."
            );
        }
        if stock_evidence && c.rejected == 0 {
            println!(
                "WARNING ({label}): no order was ever rejected: the stock never ran out, which \
                 is the only place an oversell can happen; a stock hold here says nothing about \
                 the race."
            );
        }
        let can_overlap = s.order_services >= 2 || s.variant == Variant::Direct;
        if stock_evidence && can_overlap && c.overlap == 0 {
            println!(
                "WARNING ({label}): no Check ever met another open order at the inventory: the \
                 race window was never exercised; treat a stock hold as no data."
            );
        }
        if !can_overlap {
            println!(
                "NOTE ({label}): one Kafka order service handles one order at a time, so orders \
                 never overlap at the inventory (overlap = 0 by construction); that is why this \
                 configuration holds."
            );
        }
        if stock_evidence && s.variant == Variant::V2 && s.order_services >= 2 && c.contended_reject == 0 {
            println!(
                "WARNING ({label}): no order was rejected while another was open: the atomic \
                 check-and-take was never exercised under contention; treat a hold as no data."
            );
        }
        if b.deadline.is_some() && c.in_time == 0 {
            println!(
                "WARNING ({label}): with a deadline, no reply was ever read in time: no response \
                 outcome survived; treat a deadline hold as no data."
            );
        }
    }
    if mode == Mode::Timed && orders_can_queue(s) {
        println!(
            "NOTE ({label}): orders can wait here for a busy order service, and timed mode keeps \
             a waiting message readable only sd past its arrival (this model's broker \
             retention): a small sd silently discards real executions. Compare execs with \
             --mode baseline and raise --sd-ratio until they agree."
        );
    }
    if b.deadline.is_none() && blocked > 0 && !violated {
        match mode {
            Mode::Baseline => {
                println!(
                    "DEADLOCK ({label}): blocked={blocked} with only blocking receives in \
                     untimed mode means a genuine deadlock; this run is not a hold (exit 3)."
                );
                return true;
            }
            Mode::Timed => println!(
                "NOTE ({label}): blocked={blocked} with no deadline: in timed mode these are \
                 executions that lose a message (a reader took a later arrival first, which \
                 drops the earlier one, or a queued order outlived its storage lifetime sd), \
                 not behaviours of the exactly-once broker; compare execs with baseline."
            ),
        }
    }
    false
}

/// Exit code for a genuine untimed deadlock (see the doc header).
const EXIT_DEADLOCK: i32 = 3;

// =====================================================================
// Reporting
// =====================================================================

fn deadline_str(b: Bounds) -> String {
    b.deadline.map_or_else(|| String::from("none"), |d| d.to_string())
}

fn print_one(label: &str, s: Setup, b: Bounds, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<9} variant={v} N={n} assign={a} C={cu} R={r} S={st} P={p} Q={q}  L={l} U={u} \
         sd={sd} D={d}  execs={execs:<7} blocked={block:<7} accepted={acc:<6} rejected={rej:<6} \
         overlap={ov:<6} race_window={rw:<6} contended_reject={cr:<6} oversold={os:<5} \
         in_time={it:<6} late={late:<5} time={dur:?}",
        v = s.variant.name(), n = s.order_services, a = s.assign.name(), cu = s.customers,
        r = s.rounds, st = s.stock, p = s.products, q = s.max_qty,
        l = b.l, u = b.u, sd = b.sd, d = deadline_str(b),
        execs = stats.execs, block = stats.block,
        acc = c.accepted, rej = c.rejected, ov = c.overlap, rw = c.race_window,
        cr = c.contended_reject, os = c.oversold, it = c.in_time, late = c.late,
    );
}

fn print_compare(s: Setup, b: Bounds, baseline: (Stats, Duration), timed: (Stats, Duration)) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("Online retail stock race: MUST vs MUST-timed");
    println!("======================================================");
    println!(
        "variant = {}    N = {}    assign = {}    C = {}    R = {}    S = {}    L = {}    U = {}    sd = {}    D = {}",
        s.variant.name(), s.order_services, s.assign.name(), s.customers, s.rounds, s.stock,
        b.l, b.u, b.sd, deadline_str(b)
    );
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
    println!();
    // Executions EXPLORED is execs + block: a violating execution is aborted
    // by its assertion and an evicted one blocks, so both are filed under
    // `block`. Dividing complete executions alone overstates the reduction
    // (sensor network defaults: 7702x that way, 215x explored).
    let b_explored = b_stats.execs + b_stats.block;
    let t_explored = t_stats.execs + t_stats.block;
    let exec_ratio = b_explored as f64 / t_explored.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "explored reduction: {exec_ratio:.2}x  ({b_explored} vs {t_explored} executions explored, execs+blocked)"
    );
    println!("time  speedup  : {time_ratio:.2}x");
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    setup: Setup,
    /// None = the variant's default (2 for v1/v2, 1 for direct).
    order_services: Option<usize>,
    u: u64,
    l_ratio: f64,
    sd_ratio: f64,
    deadline_ratio: Option<f64>,
    keep_going: bool,
}

fn next_val(args: &mut std::env::Args, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
}

fn parse_num<T: std::str::FromStr>(v: String, flag: &str) -> T {
    v.parse()
        .unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::from("timed"),
        setup: Setup {
            variant: Variant::V1,
            order_services: DEFAULT_ORDER_SERVICES,
            assign: Assign::Fixed,
            customers: DEFAULT_CUSTOMERS,
            rounds: DEFAULT_ROUNDS,
            stock: DEFAULT_STOCK,
            products: DEFAULT_PRODUCTS,
            max_qty: DEFAULT_MAX_QTY,
        },
        order_services: None,
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        deadline_ratio: None,
        keep_going: false,
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--variant" => {
                a.setup.variant = match next_val(&mut args, "--variant").as_str() {
                    "v1" => Variant::V1,
                    "v2" => Variant::V2,
                    "direct" => Variant::Direct,
                    other => cli_bail(&format!("invalid --variant: {other} (expected v1|v2|direct)")),
                }
            }
            "--order-services" => {
                a.order_services =
                    Some(parse_num(next_val(&mut args, "--order-services"), "--order-services"))
            }
            "--assign" => {
                a.setup.assign = match next_val(&mut args, "--assign").as_str() {
                    "fixed" => Assign::Fixed,
                    "any" => Assign::Any,
                    other => cli_bail(&format!("invalid --assign: {other} (expected fixed|any)")),
                }
            }
            "--customers" => {
                a.setup.customers = parse_num(next_val(&mut args, "--customers"), "--customers")
            }
            "--rounds" => a.setup.rounds = parse_num(next_val(&mut args, "--rounds"), "--rounds"),
            "--stock" => a.setup.stock = parse_num(next_val(&mut args, "--stock"), "--stock"),
            "--products" => {
                a.setup.products = parse_num(next_val(&mut args, "--products"), "--products")
            }
            "--max-qty" => a.setup.max_qty = parse_num(next_val(&mut args, "--max-qty"), "--max-qty"),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--deadline-ratio" => {
                a.deadline_ratio =
                    Some(parse_num(next_val(&mut args, "--deadline-ratio"), "--deadline-ratio"))
            }
            "--keep-going" => a.keep_going = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: retail_saga_timed [--mode baseline|timed|compare] \
                     [--variant v1|v2|direct] [--order-services N] [--assign fixed|any] \
                     [--customers C] [--rounds R] [--stock S] [--products P] [--max-qty Q] \
                     [--u U] [--l-ratio LR] [--sd-ratio SR] [--deadline-ratio DR] [--keep-going]\n\
                     Defaults: mode=timed variant=v1 N=2 (1 for direct) assign=fixed C=2 R=2 S=5 P=1 Q=2 U=1 \
                     L/U=0 sd/U=0, no deadline.\n\
                     Exit 0 = no violation over the explored state space; exit 101 = stock went \
                     below zero (or, with a deadline, a reply was late); exit 2 = CLI misuse; \
                     exit 3 = genuine deadlock (blocked > 0 in an untimed run without a deadline)."
                );
                std::process::exit(0);
            }
            other => cli_bail(&format!("unknown argument: {other}")),
        }
    }
    a
}

fn main() {
    let a = parse_args();
    let mut s = a.setup;
    s.order_services = a.order_services.unwrap_or(match s.variant {
        // Section 2 (Listing 2.5) has exactly one order service.
        Variant::Direct => DIRECT_ORDER_SERVICES,
        _ => DEFAULT_ORDER_SERVICES,
    });
    if s.order_services < 1 || s.order_services > MAX_ORDER_SERVICES {
        cli_bail("--order-services must be in 1..=4");
    }
    if s.customers < 1 {
        cli_bail("need at least 1 customer");
    }
    if s.rounds < 1 {
        cli_bail("need at least 1 round");
    }
    if s.stock < 0 {
        cli_bail("--stock must be >= 0");
    }
    if s.products < 1 {
        cli_bail("--products must be >= 1");
    }
    if s.max_qty < 1 {
        cli_bail("--max-qty must be >= 1");
    }
    let b = Bounds::from_ratios(a.u, a.l_ratio, a.sd_ratio, a.deadline_ratio);
    let deadlock = match a.mode.as_str() {
        "baseline" => {
            let (st, d) = run(Mode::Baseline, s, b, a.keep_going);
            let c = read_counts();
            print_one("baseline", s, b, &st, d, c);
            warn_if_vacuous("baseline", s, b, Mode::Baseline, st.execs, st.block, c)
        }
        "timed" => {
            let (st, d) = run(Mode::Timed, s, b, a.keep_going);
            let c = read_counts();
            print_one("timed", s, b, &st, d, c);
            warn_if_vacuous("timed", s, b, Mode::Timed, st.execs, st.block, c)
        }
        "compare" => {
            let baseline = run(Mode::Baseline, s, b, a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, s, b, a.keep_going);
            let tc = read_counts();
            print_one("baseline", s, b, &baseline.0, baseline.1, bc);
            print_one("timed", s, b, &timed.0, timed.1, tc);
            let (b_vac, t_vac) = (
                (baseline.0.execs, baseline.0.block),
                (timed.0.execs, timed.0.block),
            );
            print_compare(s, b, baseline, timed);
            let b_dead = warn_if_vacuous("baseline", s, b, Mode::Baseline, b_vac.0, b_vac.1, bc);
            let t_dead = warn_if_vacuous("timed", s, b, Mode::Timed, t_vac.0, t_vac.1, tc);
            b_dead || t_dead
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    };
    if deadlock {
        std::process::exit(EXIT_DEADLOCK);
    }
}
