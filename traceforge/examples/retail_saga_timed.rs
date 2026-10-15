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
//! Lost messages. The checker does not assume a safe network: any message
//! may be lost on the way, in both modes, and nothing in this file
//! prevents that or looks away from it. A lost message can only take an
//! order away, so it never causes an oversell. It does leave a shopper
//! without an answer: without a deadline that shopper waits for ever and
//! the run is reported as a deadlock; with a deadline the shopper gives
//! up and the run is reported as a late answer.
//!
//! Timers. The original design has no timers at all, and neither does
//! this example unless you ask for one:
//!
//!   * In timed mode every message takes some time to travel, somewhere
//!     between a shortest and a longest travel time, and a message that
//!     sits unread for too long after it arrives is thrown away, which
//!     for the shop is one more lost message. A receiver that is waiting
//!     always takes the message that arrived first among those it can
//!     still read (messages that arrive at the same instant may be taken
//!     in either order), so a message can only be thrown away unread when
//!     its receiver is not waiting for it: a service in the middle of
//!     another order, or a shopper that has already given up. The program
//!     is exactly the same as in untimed mode; only the checker's view of
//!     time changes. The trip to the shipping desk and back is counted as
//!     travel time.
//!   * Optionally, a shopper gives up waiting for an answer after a set
//!     time (a checkout deadline). The checker then reports whether a
//!     shopper could ever be left without an answer for that long. One
//!     lost message is enough for that, so the answer is yes at every
//!     deadline.
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
//! Threads (default 5 besides main). Main spawns the inventory, then the
//! order services (each gets the inventory's id through its spawn
//! closure), then the customers (each gets the services' ids), and joins
//! the customers only. No thread is a daemon:
//!
//!   Customer c (x C)        ordinary thread. Listing 4.2: R orders, one
//!                           at a time; each order picks a product in
//!                           0..P and an amount in 1..=Q (the models'
//!                           ?(0,1) and ?(1,2)), then waits for the Reply
//!                           on its own topic. It ends after its R-th
//!                           Reply (or at a deadline, see Properties).
//!   OrderService k (x N)    ordinary thread. Listing 4.3 (v1) or 4.4
//!                           (v2): getMessage is a blocking receive for
//!                           the next Order; while an order is open it
//!                           reads ONLY from the inventory (and its own
//!                           shipping message), so queued orders wait,
//!                           exactly like the Kafka consumer that asks
//!                           for the next message only after answering.
//!                           It ends after the orders it is owed.
//!   Inventory (x 1)         ordinary thread. Listing 2.3 (v1) or 4.4
//!                           (v2); stock[p] = S. It ends after the
//!                           Checks and Confirms it is owed.
//!
//! The bound of every thread is the count of messages that a run in which
//! no message is lost delivers to it, so a thread that ends blocked is
//! waiting for a message that was lost or expired. A customer ends after
//! its R Replies. The inventory is owed one Check per order (C x R in all)
//! and, in v1 and direct, one Confirm per approved order; how many orders
//! are approved depends on the stock, so it keeps the approvals whose
//! Confirm it has not handled yet, and ends when no Check is due and no
//! approval is waiting for its Confirm. An order service is owed the
//! orders of its customers: R for each customer c with c mod N = k under
//! `--assign fixed` (or with one service). A Kafka service serves them
//! one after the other; the direct service ends once it has read all of
//! them and answered every one it has read. Under `--assign any` with
//! two or more services the customer's own nondeterministic choice
//! decides how many orders a service gets, which the service cannot
//! know, so there main sends each service a HARNESS Terminate after
//! joining the customers (see No harness). Without a failed assertion, an
//! execution is complete when every thread has ended, and blocked when a
//! thread waits for a message that was lost or expired.
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
//! filtering is by sender: Kafka services read orders with `sender !=
//! inventory && sender != self`, the inventory's answers with `sender ==
//! inventory` and their shipping message with `sender == self`. The
//! customers, the inventory and the direct service take whatever they are
//! sent.
//!
//! `--variant direct` is the Section 2 model (Listings 2.1 to 2.5, no
//! broker): the order service is purely reactive and keeps several
//! orders open at once, with the order data carried in every message
//! (the listing's `cu, product, qty` parameters). The inventory echoes
//! `customer, product, qty` in all variants; the Kafka services ignore
//! the echo, so this changes no behaviour.
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
//!     (customer c always uses service c mod N) is this port's cost
//!     choice, not the paper's: it does not hide the race, because the
//!     race needs two orders open at different services at once, and a
//!     customer has only one open order, so those two orders always come
//!     from different customers (v1 FIREs under both assignments, see
//!     Results). Transit [L,U] is read as the whole
//!     producer-to-consumer trip through the broker.
//!   * The listing's buffer code is not copied. Listing 4.1 lines 40-44
//!     (and both local models) shift the topic buffer using `size`
//!     instead of `cnt`, so the head slot is never overwritten after a
//!     consume: with 2 queued messages the first is delivered twice and
//!     the second is lost. This contradicts the paper's own "exactly-once"
//!     and "first-in-first-out" statements. Of that stated intent the
//!     channels here keep "at most once" and "in order"; whether an order
//!     arrives at all is the checker's semantics (see the last item).
//!   * ShippingService is not a thread. Its arrangeDelivery /
//!     shippingIsScheduled round trip is a single self-addressed
//!     ShippingScheduled message sent with send_msg_timed over
//!     [2L, 2U + sd] (outbound hop, the desk's read slack sd, return
//!     hop; the service's own read slack is added by the checker at the
//!     read). The service stays busy until it reads it, as in Listing 4.3
//!     where getMessage follows shippingIsScheduled. Like every send it
//!     may be lost; a lost self-message stands for a lost hop of that
//!     round trip. In untimed mode it is an ordinary self-message
//!     (send_msg_timed is inert by library design), so both modes verify
//!     the same program. Real shipping services take time to book a
//!     delivery; this keeps that cost.
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
//!     `--assign any`, or more customers than services), an order that
//!     waits longer than sd expires unread. That is a lost message like
//!     any other: the shopper who sent it gets no answer. A larger
//!     `--sd-ratio` keeps queued orders readable for longer; the binary
//!     prints a NOTE in these configurations.
//!   * Optional `--deadline-ratio`: a checkout request timeout at the
//!     customer, a bounded reading of the local .property files'
//!     untimed formula G(!c1Buying || F(!c1Buying)). It is not in the
//!     paper (which claims deadlock freedom instead), is off by default,
//!     and a real storefront does have such a timeout.
//!   * Not modelled, because neither the paper nor the models have them:
//!     reservation expiry (v2 takes the items immediately, there is no
//!     reservation), consumer rebalancing or redelivery timers,
//!     at-least-once duplicates, crashes.
//!   * Message loss is not in the paper, and it IS in this model. Rebeca's
//!     mailboxes and the paper's broker lose nothing. Under the checker's
//!     semantics every send may be lost, with no budget, in both modes,
//!     and in timed mode a message that is not read within sd of its
//!     arrival expires, which for the shop is the same thing. The file
//!     adds nothing to that and removes nothing from it: no scripted
//!     loss, no loss budget, no filter (see No harness). What it does to
//!     each property is stated under Properties checked.
//!
//! ## No harness (since 2026-10-13)
//!
//! The file contains the protocol and nothing else: every message is an
//! order, a stock request or an answer of the listings, and no code
//! filters executions. The one exception is the Terminate that main sends
//! to each service under `--assign any` with two or more services, whose
//! order count no service can know (see the bound of the run): it is
//! marked HARNESS in the code, is a plain lossy send, and filters
//! nothing. In baseline mode it only adds blocked executions (the run in
//! which it is lost). In timed mode a join does not advance main's
//! clock, so it is sent at instant 0, arrives by U, and a service that
//! has done work starts its last wait later than that: unless sd covers
//! the gap it expires unread, which adds more blocked executions, and
//! with L > 0 and a small sd no timeline lets a busy service read it, so
//! the cell reports no complete execution (the 0-executions WARNING).
//! Verdicts are not affected: a violating execution never reaches the
//! send. Until 2026-10-13 the file carried three pieces of verification
//! harness:
//!
//!   * Init messages (OsMsg::Init, CustMsg::Init), by which main told
//!     every thread the ids it needed. Ids now travel through the spawn
//!     closures. Like every send an Init could be lost, and its thread
//!     then never started.
//!   * Done messages (OsMsg::Done, InvMsg::Done), by which a customer
//!     released its service(s) after its R orders and a service released
//!     the inventory, so that the reactive threads terminated and main
//!     could join them. A lost Done left a service waiting for ever, and
//!     the execution blocked although no shopper was waiting for
//!     anything. The services and the inventory now end on the count of
//!     messages that a run without a lost message delivers to them (see
//!     the bound of the run), so only a lost message leaves one blocked.
//!   * The timeout validation read (assume_reply_was_late). After its
//!     deadline the customer made a BLOCKING read of the very Reply it
//!     had been waiting for, so that a timeout counted only if that
//!     Reply was eventually read. The timing of a timeout is judged by
//!     the checker itself: a timeout branch is a real execution exactly
//!     where no matching message could have arrived during the whole
//!     wait, judged when the execution is complete. What the extra read
//!     still filtered out was every execution in which the Reply, or any
//!     message before it, was LOST or expired unread: such a Reply is
//!     never read, so the read waited for ever and the execution was
//!     discarded as blocked. Under the model's semantics every send may
//!     be lost, and a shopper without an answer at the deadline is late
//!     whether the answer was slow or lost. The read is gone, the
//!     assertion sits directly on the timeout branch, and the boundaries
//!     that depended on the read are withdrawn (see Results).
//!
//! ## Properties checked
//!
//!   SafeStockLevel (paper Section 4.3, both .property files):
//!       stock[p] >= 0 for every product p. Checked at the inventory
//!       right after every decrement with traceforge::assert. Stock is
//!       local state of one thread, so the check needs no clock and no
//!       global observer. A lost message takes an order away or stops a
//!       service; it never adds a decrement, so loss cannot cause an
//!       oversell.
//!   Bounded response (optional, deviation above): a customer reads the
//!       Reply to an order before D has passed since it sent the Order.
//!       The customer waits Finite(D) for the Reply; on the timeout
//!       branch it counts the miss and asserts false, there and then,
//!       and stops ordering. The checker admits that branch only where no
//!       Reply could have arrived during the whole wait (a Reply that
//!       arrives exactly at the deadline may be read or missed, both are
//!       explored). A Reply that never comes, because it or any message
//!       before it was lost or expired, is a late Reply: no condition
//!       exempts it.
//!   Deadlock freedom (paper: "no deadlock"): without a deadline every
//!       receive is blocking, as in the listings. A shopper whose Order,
//!       Reply or any message in between is lost waits for ever, and the
//!       execution ends blocked (so does the service or the inventory
//!       that waits for the same lost message: no thread is a daemon).
//!       So blocked > 0 without a deadline and without a violation is a
//!       deadlock: the binary prints a DEADLOCK line and exits 3
//!       (distinct from a hold's 0 and a violation's 101), by the same
//!       rule in both modes. The paper's claim is about Rebeca mailboxes,
//!       which lose nothing. Under semantics that may lose any message,
//!       shoppers and services that wait without a timeout cannot be
//!       deadlock free, and the DEADLOCK verdict says exactly that.
//!
//! ## Evidence counters and vacuity warnings
//!
//! Static counters, reset before each verify, accumulated over all
//! explored executions (revisited prefixes may re-count: treat them as
//! zero/nonzero evidence). They are observation only: each is raised at
//! the site where its event happens, and no protocol decision reads one.
//!
//!   accepted / rejected   Status replies with available = true / false.
//!                         rejected > 0 proves stock exhaustion was
//!                         reached, the only place an oversell can occur.
//!   overlap               the inventory handled a Check while another
//!                         order was open at some service (a global
//!                         in-flight count, evidence only, never read by
//!                         protocol logic; an order whose service is
//!                         stuck behind a lost message stays open).
//!   race_window           (v1, direct) the inventory answered a Check
//!                         while another order of the same product had
//!                         been approved but not yet taken off the count:
//!                         the check-then-act gap was open. Local to the
//!                         inventory thread.
//!   contended_reject      a Check was rejected while another order was
//!                         open: in v2 this is the would-be oversell that
//!                         the atomic step turned into a refusal.
//!   oversold              a count went below zero (counted just before
//!                         the assert, so --keep-going reports how often).
//!   in_time / late        Replies read before the deadline / deadlines
//!                         that passed without a Reply, late or lost
//!                         (counted just before the assert, one per
//!                         shopper).
//!
//! Where --keep-going files a violating execution. The stock assertion
//! stops the inventory, so that execution counts as blocked. A shopper
//! that gives up at its deadline fails its assertion and ends its thread,
//! so that execution counts under execs when nothing else blocks it
//! (blocked = 0 in every deadline cell measured). Either way the tool
//! prints one line naming the failing task per violating execution
//! (task-N in spawn order: 1 the inventory, then the services, then the
//! customers). That line count is the number of violating executions;
//! oversold and late count events.
//!
//! The binary prints a WARNING when a run cannot count as a hold:
//! execs = 0; rejected = 0 (stock never ran out); overlap = 0 with two or
//! more services or in direct mode (orders never met at the inventory);
//! contended_reject = 0 for v2 with two or more services; a deadline run
//! with in_time = 0. The rejected, overlap and contended_reject warnings
//! are evidence for the stock property only, so a deadline run skips them
//! (its cells use a large stock on purpose, so no order is ever rejected)
//! and prints a NOTE instead: a deadline hold would be judged by
//! execs > 0, in_time > 0 and late = 0. No deadline run gets that far
//! while every send may be lost (late is never 0); the two deadline
//! guards stay so that an exit 0 could never be read as a hold. It prints
//! a NOTE for a single Kafka service (overlap is 0 by construction) and,
//! in timed mode, where orders can queue (an order that outlives sd
//! expires, which is a lost message). For blocked > 0 without a deadline
//! and without a violation it prints a DEADLOCK line and exits 3, in both
//! modes. With --keep-going a VIOLATIONS line replaces the hold warnings
//! when oversold > 0 or late > 0.
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
//! Exit codes: 0 = no violation and no deadlock over the explored state
//! space; 101 = a property violation (stock below zero, or a deadline
//! that passed without a Reply); 2 = CLI misuse (including a negative or
//! non-finite ratio), so exit-code-based sweep harnesses cannot mistake a
//! typo for a FIRE; 3 = a deadlock (blocked > 0 in a run without a
//! deadline and without a violation, in either mode: a shopper waits for
//! ever), so a deadlock is never recorded as a hold. With --keep-going a
//! violation is reported by the VIOLATIONS line and the exit code is 0.
//!
//! ## Results (2026-10-13, re-measured after the first-arrival ruling)
//!
//! Every number below was measured (one worker, `timeout 300`, on a
//! machine shared with other runs); none is a prediction and none is
//! carried over from the harness version. "old" is that version, HEAD
//! 0cbe2c7, on the same cell. All new numbers are of this file on the
//! engine as it stands after the ruling of 2026-10-13 on timed receives:
//! a receive that reads a message reads the one that ARRIVED first among
//! the matching messages pending at it (a tie leaves both orders), and
//! only a message already discarded when the wait began (its sd had run
//! out) is passed over. Baseline mode has no clock and is not touched by
//! the rule. Before the ruling the same code had more timed executions,
//! because a waiting receive could take a later arrival while an earlier
//! one expired unread (33,720 blocked instead of 27,964 in the v1 cell,
//! 60 executions instead of 52 for v2 at D >= 7); no verdict and no
//! baseline count differs. A cell is written as the flags that follow
//! `retail_saga_timed --mode M`. The verdict of a cell is the exit code
//! of the run WITHOUT --keep-going: 101 = violation (in timed mode with a
//! certified witness timeline), 3 = deadlock, 0 = hold. The counts are
//! from the same cell WITH --keep-going. Default cell: C=2 R=2 S=5 P=1
//! Q=2, N=2 (1 for direct), fixed assignment, L=0 U=1 sd=0. The engine
//! reported timeline_impossible = 0 in every run that finished
//! (TF_PRINT_STATS=1), so the counters count events of real executions
//! only.
//!
//! Without a deadline (`--variant V`; the last cell adds
//! `--order-services 1`). Counts are execs / blocked / violating
//! executions (the tool's failing-task lines):
//!
//!   cell     mode      exit old/new  new counts            old counts
//!   v1       timed     101 / 101     668/27,964/1,464      2,340/67,930/2,496
//!   v1       baseline  101 / 101     668/27,964/1,464      2,340/54,478/2,496
//!   v2       timed       0 / 3       96/3,128/0            320/8,444/0
//!   v2       baseline    3 / 3       96/3,128/0            320/6,854/0
//!   direct   timed     101 / 101     11,194/182,450/2,174  42,498/522,552/2,704
//!   direct   baseline  101 / 101     11,194/182,450/2,174  42,498/384,734/2,704
//!   v2, N=1  timed       0 / 3       96/2,212/0            320/4,762/0
//!   v2, N=1  baseline    3 / 3       96/1,980/0            320/3,970/0
//!
//! (The old direct cell in timed mode with --keep-going did not finish in
//! 300 s; its counts above are from a run with a limit of 600 s, which
//! took 389 s, and another took 411 s.) Wall time of the --keep-going
//! runs, new against old, two runs each, taken one after the other on a
//! machine with a load average of 13 to 21 on 16 cores (the absolute
//! times are inflated, so only the ratios carry): v1 timed 32 to 39 s
//! against 72 to 75 s, v1 baseline 8 to 10 s against 22 s, direct
//! baseline 48 to 61 s against 121 to 127 s, v2 timed 3.0 to 3.2 s
//! against 7.1 to 7.6 s, direct timed 239 to 280 s against 389 and 411 s
//! (a limit of 300 s is tight there). The ruling costs a little in timed
//! mode: the same code before it took 32 to 34 s (v1 timed), 2.6 to 2.7 s
//! (v2 timed) and 168 to 205 s (direct timed) in the same series.
//!
//! SafeStockLevel keeps its verdicts. v1 and direct FIRE in both modes;
//! v2 never oversells (oversold = 0 over all its executions, with
//! rejected, overlap and contended_reject all > 0, so the atomic step was
//! exercised under contention). Certified witness, v1 timed: both
//! shoppers first buy 1 (5 becomes 3) and then order 2 each; the
//! inventory answers the first service's Check (3 >= 2: yes) and the
//! second service's Check (3 >= 2: yes) before either Confirm, takes 2
//! (1 left) and takes 2 again: -1. Every event of the witness is at
//! instant 0, which L = 0 allows, and no message of that execution is
//! lost. The direct witness is the same race inside the one reactive
//! service. The race needs no loss: a hand enumeration of the executions
//! of the v1 cell in which no send is dropped gives 668 that finish (the
//! engine's 668) and 368 that oversell, so 368 of the 1,464 violating
//! executions lose no message, in both modes; in 24 of them the other
//! order's Check or Confirm is still pending at the stopped inventory.
//! (Printed graphs of a copy of this file built with
//! Config::with_verbose(2) before the ruling, which leaves this cell's
//! counts unchanged, gave the same 344 + 24 split.)
//!
//! Also measured, in both modes unless noted. The minimal cell `--variant
//! v1 --rounds 1 --stock 1 --max-qty 1` FIREs (16 violating executions of
//! 108, in both modes), also with `--assign any` (witness with the two
//! customers on different services), and in timed mode at L = U = 1
//! (`--l-ratio 1`), at L = U = 2 (`--u 2 --l-ratio 1`: both Checks read
//! at 4, both Confirms at 8) and at U = 2, L = 1, sd = 1 (`--u 2
//! --l-ratio 0.5 --sd-ratio 0.5`). Nothing in the program separates the
//! two services' Checks in time, so this property has no timing boundary.
//! The paper's two products (`--products 2`) FIRE for v1 and for direct,
//! and so do 3 customers (`--customers 3`).
//! Controls in which the stock property holds (oversold = 0) and the run
//! exits 3 for the deadlock: `--variant v1 --rounds 1` (demand at most 4
//! of 5: race_window > 0 but rejected = 0, WARNING printed), `--variant
//! v1 --order-services 1` (overlap = 0 by construction) and the minimal
//! cell with `--variant v2`.
//!
//! The complete executions are now the protocol's own. v2: 96 = 16
//! choices of amounts x 6 orders in which the inventory can take the two
//! Checks of each of the two services. The old 320 = 16 x 20 also ordered
//! the two services' Done messages at the inventory.
//!
//! Deadlock. Every cell without a deadline that does not FIRE exits 3, in
//! both modes. The old version exited 3 in baseline (its Init and Done
//! messages could be lost as well) and 0 with a NOTE in timed mode. A
//! blocked execution has no witness printout, so the evidence is a hand
//! enumeration (every send delivered or dropped, the inventory taking
//! the two services' messages in either order): for the v2 cell it gives
//! 96 complete and 3,128 blocked executions, each blocked one with a
//! dropped send, which is what both modes report. No order waits for a
//! busy service in this cell, and a waiting receiver takes the earliest
//! arrival, so nothing expires unread (before the ruling timed mode had
//! 480 more blocked executions, in which a Check reached the inventory
//! and expired unread because the inventory took the other service's
//! Check). Where an order can wait for a busy service, timed mode has
//! blocked executions that baseline has not: in the one-service cell
//! 2,212 against 1,980, the 232 more being Orders that were delivered and
//! expired unread while the service was busy (printed graphs of a copy of
//! this file built with Config::with_verbose(2) before the ruling, which
//! leaves this cell's counts unchanged). Hand count, `--customers 1
//! --rounds 1 --order-services 1`: v2 gives 2 complete and 10 blocked
//! executions, v1 gives 2 and 14, in both modes: 2 amounts, times one
//! blocked execution for each of the 5 (v2) or 7 (v1) messages of an
//! accepted order as the first one lost. The old version gave 20 and 24
//! blocked in baseline: its harness added 5 per amount (3 ways to lose an
//! Init, and the 2 Dones).
//!
//! With a deadline (`--variant V --rounds 1 --stock 20 --max-qty 1
//! --deadline-ratio D`; S = 20 so that no order is rejected or oversold).
//! New counts are execs / violating executions, and blocked = 0 in every
//! new run (see Evidence counters); old counts are execs / blocked /
//! violating. Baseline was run at D = 6, 7, 8, 9 and 30 and gave the same
//! counts at each, as it must without a clock:
//!
//!   cell        mode      exit old/new  new counts  old counts
//!   v2 D=6      timed     101 / 101     74 / 72     6 / 285 / 46
//!   v2 D=7      timed       0 / 101     52 / 50     6 / 239 / 0
//!   v2 D=30     timed       0 / 101     52 / 50     6 / 239 / 0
//!   v2 6,7,30   baseline  101 / 101     74 / 72     6 / 228 / 40
//!   v1 D=8      timed     101 / 101     236 / 230   20 / 693 / 106
//!   v1 D=9      timed       0 / 101     180 / 174   20 / 587 / 0
//!   v1 D=30     timed       0 / 101     180 / 174   20 / 587 / 0
//!   v1 8,9,30   baseline  101 / 101     250 / 244   20 / 576 / 110
//!
//! Bounded response is VIOLATED at every deadline, in both modes: one
//! lost message is enough. Certified witness, v2 timed, D = 30: a shopper
//! sends its Order at 0; the order is checked, approved and shipped, and
//! the service's Reply is lost on the way (the send is marked dropped, as
//! is the other shopper's Reply); nothing can reach the shopper in
//! [0, 30), its wait runs out at 30 and the assertion fails at 30. The
//! old version discarded this execution: its validation read waited for
//! ever for the lost Reply. The same holds for v1 and direct at D = 9 and
//! 30. At a D no larger than the sum of the hop bounds (6 for v2, 8 for
//! v1 and direct) the certified witness is instead the late Reply of the
//! next paragraph.
//!
//! What the executions are (hand counts, which the measured counts
//! confirm; v2 cell first). Each order ends in one of six ways: nothing
//! lost, or its first, second, ..., fifth send lost. Two orders give 36
//! loss patterns, and a pattern in which both Checks reach the inventory
//! is run twice, once for each order in which the inventory can take
//! them: 36 + 16 = 52 executions, 2 of which (nothing lost, both Replies
//! read) do not violate. That is the timed count at D = 7 and at D = 30,
//! and each of its 50 violating executions has a message lost in transit:
//! a Reply that is sent and delivered arrives within the 6 time units of
//! a v2 order, so no shopper can miss it at D >= 7. Baseline has 22 more
//! (74): a shopper takes its timeout although its Reply is on its way, 16
//! of them with a message of the other order lost as well, which only a
//! checker without a clock admits. At D = 6, the sum of the six hop
//! bounds of a v2 order at U = 1, timed mode has all 74: a Reply that
//! arrives exactly at the deadline may be missed (the certified witness
//! of that cell is one of them: no send is dropped, both Replies are
//! sent, and each shopper's wait still ends at 6 without one).
//!
//! v1 has the same shape with eight outcomes per order: 2 put no message
//! of the order at the inventory, 2 put one (its Check) and 4 put two
//! (Check and Confirm), and two orders interleave there in C(i + j, i)
//! ways. That gives 180 executions, 174 violating, at D = 9 and at D = 30,
//! each of the 174 with a message lost in transit. Baseline has 250 (244):
//! the same 174 and 70 in which a shopper takes its timeout with its Reply
//! on its way. Timed mode has all 250 at D = 6 and 236 (230) at D = 7 and
//! at D = 8: the 180 and 56 of the 70. The other 14 have the inventory
//! take all the messages of an order whose Reply is on its way (and
//! missed) before the other order's Check, which cannot happen when that
//! Reply must arrive at or after D = 7 or 8 (its Confirm then arrives
//! after instant 2, and every Check has arrived by 2). So the clock still
//! tells a slow answer from an impossible one; it no longer decides the
//! verdict.
//!
//! Also measured: the same FIRE (exit 101, certified) for v2 at U = 2, L
//! = 1, sd = 1 and D = 30 (`--u 2 --l-ratio 0.5 --sd-ratio 0.5
//! --deadline-ratio 15`; old: hold), for two rounds (`--variant v2
//! --stock 20 --deadline-ratio 30`: 3,432 violating executions of 3,528
//! timed, 5,040 of 5,136 baseline; old: hold in timed mode), and for
//! `--variant direct` at D = 8, 9 and 30 in timed mode (old: FIRE, hold,
//! hold; 379, 311 and 311 violating executions of 397, 329 and 329, and
//! 479 of 497 in baseline at each D).
//!
//! sd and queued orders (`--variant v2 --order-services 1 --l-ratio 1
//! --sd-ratio SD`, timed). At L = U = 1 both first Orders reach the one
//! service at instant 1, and the one not taken waits while the other
//! order is served, which takes at least 4 (a Check hop, a Status hop and
//! the shipping round trip of 2L = 2). Complete executions: 0 at sd in
//! {0, 1, 2, 3} (every run blocks: a message is lost, or the waiting
//! Order expires before the service is free; WARNING and DEADLOCK), 64 at
//! sd in {4, 5, 6, 20}, against 96 in baseline and in timed mode at L = 0.
//! 64 = 16 choices of amounts x 4 of the 6 orders in which the service
//! can take the four Orders: the two in which one shopper is served twice
//! before the other is served once are gone, because the waiting Order
//! arrived before that shopper's second one and is still readable
//! (sd >= 4), so the service must take it first.
//!
//! WITHDRAWN. Everything this header reported before 2026-10-13 was
//! measured with the harness and is no longer claimed. (1) The
//! bounded-response boundaries "timed FIRE iff D <= 8(U + sd)" for v1 and
//! direct and "iff D <= 6(U + sd)" for v2, with their corollaries (the
//! atomic fix shortens the worst response by 2(U + sd); the boundary does
//! not depend on L or R; a hold keeps every completed execution): they
//! described the executions the validation read kept, those in which
//! every message of the order is delivered and read. That sum of hop
//! bounds is still where the counts above change (the Reply that arrives
//! exactly at the deadline), but loss fires at every D, so it separates no
//! verdict. (2) "Deadlock freedom: every untimed hold has blocked = 0": it
//! predates lossy sends (2026-09-30); the old binary itself exits 3 in
//! baseline on the v2 cell, and reported the same kind of run as a NOTE in
//! timed mode. (3) The execution counts and wall times of the old tables:
//! they counted the orderings of the Done messages and the runs lost to a
//! dropped Init or Done.
//!
//! ## Note on Table 1's "Without Kafka" rows
//!
//! Table 1 reports the Section 2 model (no broker, 1 order service) as
//! correct for 2 and 3 customers, and the paper names the property: the
//! introduction to Section 4 says of the Section 2 model "Model checking
//! with Afra proves that there is no deadlock in this model and
//! additionally we verify that the number of available items never falls
//! below zero (see Sect. 4.3)". Our encoding of Listings 2.1 to 2.5
//! (`--variant direct`) breaks the second half of that claim: it FIREs at
//! the paper's own values (S=5, Q=2, with one product and with the
//! paper's two; also with 3 customers, one product) with a certified
//! counterexample in which no message is lost (read in full on the
//! one-product cell with 2 customers). The Listing 2.2
//! OrderService is purely reactive, so it can forward a second
//! customer's checkAvailability before the first order's
//! confirmBoughtItem, which is exactly the Kafka 2O race. The authors'
//! Afra model may differ from the printed listings (for example in queue
//! size bounds; the local OnlineRetailSystem.rebeca has no .property
//! file), so this should be checked against their model before calling
//! the paper wrong. The first half, "no deadlock", is a statement about
//! mailboxes that lose nothing. This model's DEADLOCK verdicts come from
//! lost messages and say nothing against it: on `--variant direct
//! --rounds 1` (exit 3 in both modes, 72 complete executions) every
//! blocked execution has a message lost in transit. A hand enumeration
//! (every send delivered or dropped, the one reactive service taking its
//! messages in any order) gives 72 complete and 1,244 blocked executions,
//! each blocked one with a dropped send, which is what both modes report
//! (before the first-arrival ruling timed mode reported 1,612, the 368
//! more being executions in which a delivered message expired unread
//! behind a later arrival).

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
// the Reply; an order whose service is stuck behind a lost message stays
// open). Reset at the start of every execution inside the verify
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
    /// Kafka order topic (Listing 4.2 line 14) or placeOrder (Listing 2.1).
    Order { customer: ThreadId, product: usize, qty: i64 },
    /// availabilityStatus (Listings 2.3 / 4.3 / 4.4).
    Status { customer: ThreadId, available: bool, product: usize, qty: i64 },
    /// confirm (v1 and direct only).
    Confirmed { customer: ThreadId, product: usize, qty: i64 },
    /// The folded arrangeDelivery / shippingIsScheduled round trip,
    /// self-addressed (see deviations).
    ShippingScheduled { customer: ThreadId },
    /// HARNESS: not a message of the listings. Main sends one to each
    /// service once every customer has ended, and only under `--assign
    /// any` with two or more services, where no service knows how many
    /// orders it is owed (see `service_quota`). The service leaves its
    /// loop on reading it.
    Terminate,
}

/// Everything the inventory receives.
#[derive(Clone, Debug, PartialEq)]
enum InvMsg {
    /// checkAvailability. `from` is Rebeca's `sender`.
    Check { from: ThreadId, customer: ThreadId, product: usize, qty: i64 },
    /// confirmBoughtItem (v1 and direct only).
    Confirm { from: ThreadId, customer: ThreadId, product: usize, qty: i64 },
}

/// Everything a customer receives.
#[derive(Clone, Debug, PartialEq)]
enum CustMsg {
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

/// Orders that a run without a lost message delivers to service `k`, and
/// so the bound of its loop. Customer c uses service c mod N under
/// `--assign fixed`, and every order goes to the one service when N = 1:
/// R orders for each customer assigned to it. Under `--assign any` with
/// two or more services the customer's own nondeterministic choice
/// decides the count, which no service can know: None, and the service
/// ends on the HARNESS Terminate that main sends it (see `run`).
fn service_quota(s: Setup, k: usize) -> Option<usize> {
    if s.assign == Assign::Any && s.order_services > 1 {
        return None;
    }
    let customers = (0..s.customers).filter(|c| c % s.order_services == k).count();
    Some(customers * s.rounds as usize)
}

// =====================================================================
// Customer (Listings 2.1 and 4.2): an ordinary thread. It places R
// orders, one at a time, and ends; main joins the customers only.
// =====================================================================

fn customer(index: usize, services: Vec<ThreadId>, s: Setup, b: Bounds) {
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
        // of its previous request"). Only order services write to a
        // customer, and only the Reply to its one open order.
        match b.deadline {
            None => {
                // The listing's wait, which has no timeout. If the Order,
                // the Reply or any message in between is lost, this
                // shopper waits for ever and the execution ends blocked
                // (the deadlock rule, see warn_if_vacuous).
                let CustMsg::Reply { .. } = traceforge::recv_msg_block_timed::<CustMsg>();
            }
            Some(d) => match traceforge::recv_msg_timed::<CustMsg>(WaitTime::Finite(d)) {
                Some(CustMsg::Reply { .. }) => {
                    REPLIES_IN_TIME.fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    // Bounded response is violated here and now: D has
                    // passed since the Order and no Reply was read. To the
                    // shopper a lost Reply (or a lost message anywhere on
                    // the way) is a late Reply; nothing is re-read.
                    LATE_REPLIES.fetch_add(1, Ordering::Relaxed);
                    traceforge::assert(false);
                    return;
                }
            },
        }
    }
}

// =====================================================================
// Order service, Kafka variants (Listings 4.3 and 4.4): an ordinary
// thread. It serves one order at a time and ends after the `quota`
// orders it is owed (service_quota). Its three inner waits (Status,
// Confirmed, its own ShippingScheduled) are the listing's: they have no
// timeout, so a service whose awaited message is lost stays in that
// order for good, blocked, and answers nobody any more.
// =====================================================================

fn read_inventory_reply(inv: ThreadId) -> OsMsg {
    traceforge::recv_tagged_msg_block_timed::<_, OsMsg>(move |snd, _| snd == inv)
}

fn kafka_order_service(inv: ThreadId, variant: Variant, b: Bounds, quota: Option<usize>) {
    let me = thread::current().id();
    let mut served = 0usize;

    // `quota` is None only under `--assign any` with two or more
    // services: the loop then ends on the HARNESS Terminate below.
    while quota.map_or(true, |q| served < q) {
        // getMessage(ORDER_TOPIC): the next order. Only customers write
        // to the order topic.
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
                served += 1;
            }
            // HARNESS: termination message (see OsMsg::Terminate).
            OsMsg::Terminate => break,
            // Unreachable: customers send only Orders.
            m => panic!("service: unexpected {m:?}"),
        }
    }
}

// =====================================================================
// Order service, no broker (Listing 2.2): an ordinary thread. It reacts
// to each message as it arrives, may have several orders open at once,
// and ends when it has read the `quota` orders it is owed
// (service_quota) and answered every one of them.
// =====================================================================

fn direct_order_service(inv: ThreadId, b: Bounds, quota: Option<usize>) {
    let me = thread::current().id();
    // Orders read so far, and orders read and not yet answered with a
    // Reply (this service's own share of IN_FLIGHT_ORDERS).
    let mut read = 0usize;
    let mut open = 0usize;

    // A lost Status, Confirmed or ShippingScheduled leaves its order open
    // for good, so the thread stays blocked. `quota` is None only under
    // `--assign any` with two or more services: the loop then ends on the
    // HARNESS Terminate below.
    while quota.map_or(true, |q| read < q) || open > 0 {
        match traceforge::recv_msg_block_timed::<OsMsg>() {
            OsMsg::Order { customer, product, qty } => {
                // placeOrder
                IN_FLIGHT_ORDERS.fetch_add(1, Ordering::Relaxed);
                read += 1;
                open += 1;
                traceforge::send_msg(inv, InvMsg::Check { from: me, customer, product, qty });
            }
            OsMsg::Status { customer, available, product, qty } => {
                // availabilityStatus
                if available {
                    traceforge::send_msg(inv, InvMsg::Confirm { from: me, customer, product, qty });
                } else {
                    traceforge::send_msg(customer, CustMsg::Reply { success: false });
                    IN_FLIGHT_ORDERS.fetch_sub(1, Ordering::Relaxed);
                    open -= 1;
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
                open -= 1;
            }
            // HARNESS: termination message (see OsMsg::Terminate).
            OsMsg::Terminate => break,
        }
    }
}

// =====================================================================
// Inventory (Listings 2.3 and 4.4): an ordinary thread. It answers until
// every order has put its Check here and every approval has been taken
// off the count by its Confirm.
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
    // Approvals given (Status true) whose Confirm has not been handled
    // yet, per product. Always 0 in v2. Race evidence, and the Confirms
    // this thread is still owed.
    let mut approved_not_taken = vec![0usize; s.products];
    // Checks still due: every order of every customer puts one here.
    let mut checks_due = s.customers * s.rounds as usize;

    // Ends when no Check is due and no approval waits for its Confirm. A
    // lost message leaves one of the two open for good, and the thread
    // blocked.
    while checks_due > 0 || approved_not_taken.iter().any(|&n| n > 0) {
        match traceforge::recv_msg_block_timed::<InvMsg>() {
            InvMsg::Check { from, customer, product, qty } => {
                checks_due -= 1;
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
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, b: Bounds, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX);
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
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
        // Thread ids travel through the spawn closures: the inventory
        // first, then the services (they know the inventory), then the
        // customers (they know the services). No thread is a daemon: the
        // customers end after their R orders, and the inventory and the
        // services end on the orders they are owed (service_quota, and
        // the inventory's own counts). Main joins the customers only.
        let inv = thread::spawn(move || inventory(s)).thread().id();
        let mut services = Vec::new();
        for k in 0..s.order_services {
            let quota = service_quota(s, k);
            let service = thread::spawn(move || match s.variant {
                Variant::Direct => direct_order_service(inv, b, quota),
                v => kafka_order_service(inv, v, b, quota),
            });
            services.push(service.thread().id());
        }
        let mut customers = Vec::new();
        for c in 0..s.customers {
            let services = services.clone();
            customers.push(thread::spawn(move || customer(c, services, s, b)));
        }
        for h in customers {
            let _ = h.join();
        }
        // HARNESS: a service without a quota (--assign any, two or more
        // services) cannot know that the run is over, so main, after its
        // last obligation (the joins above), tells each one. A plain
        // send, lossy like every send: a service whose Terminate is lost
        // stays blocked. Main's clock is still 0 here (a join does not
        // advance it), see No harness in the header.
        if service_quota(s, 0).is_none() {
            for &service in &services {
                traceforge::send_msg(service, OsMsg::Terminate);
            }
        }
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
/// is a deadlock (no deadline, blocked > 0, no violation; the same rule
/// in both modes); main then exits 3 so a deadlock is never read as a
/// hold. `mode` is read only by the note about sd, which exists only in
/// timed mode.
fn warn_if_vacuous(label: &str, s: Setup, b: Bounds, mode: Mode, execs: usize, blocked: usize, c: Counts) -> bool {
    // Only reachable with --keep-going: without it a violation exits 101.
    let violated = c.oversold > 0 || c.late > 0;
    if violated {
        println!(
            "VIOLATIONS ({label}, --keep-going): oversold={} late={}: this run is a FIRE, not \
             a hold. An execution that oversold is filed under blocked (as is, without a \
             deadline, one in which a shopper waits for ever for a lost message); one in which \
             a shopper gave up at its deadline is filed under execs. The violating executions \
             are the failing-task lines printed above.",
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
                 never overlap at the inventory (overlap = 0 by construction); that is why the \
                 stock property holds in this configuration."
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
            "NOTE ({label}): orders can wait here for a busy order service, and in timed mode a \
             waiting message stays readable only sd past its arrival (this model's broker \
             retention). An order that waits longer than sd expires unread: it is a lost \
             message like any other, and its shopper is left waiting. A larger --sd-ratio \
             keeps queued orders readable for longer."
        );
    }
    if b.deadline.is_none() && blocked > 0 && !violated {
        println!(
            "DEADLOCK ({label}): blocked={blocked} with no deadline and no violation: in each of \
             these executions a shopper waits for ever for a Reply. Under this model's semantics \
             one lost message is enough for that (every send may be lost; in timed mode a \
             message that expires unread is lost too), which is what the paper's 'no deadlock' \
             excludes for mailboxes that lose nothing. This run is not a hold (exit 3)."
        );
        return true;
    }
    false
}

/// Exit code for a deadlock: a shopper left waiting for ever, in either
/// mode (see the doc header).
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
    // Executions EXPLORED is execs + block: an execution that oversold is
    // stopped at its assertion and one whose shopper waits for a lost
    // message blocks, so both are filed under `block`. Dividing complete
    // executions alone overstates the reduction.
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
                     below zero (or, with a deadline, no reply was read in time: late or lost); \
                     exit 2 = CLI misuse; exit 3 = deadlock (blocked > 0 without a deadline and \
                     without a violation, in either mode: a shopper waits for ever for a lost \
                     message)."
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
