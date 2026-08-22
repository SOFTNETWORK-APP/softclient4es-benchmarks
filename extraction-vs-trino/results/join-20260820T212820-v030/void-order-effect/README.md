# Void: J0 and J2 measured with flight always first, and J2 unstable

Both scenarios ran flight-then-trino with no counter-balancing, and J2's trino arm
degraded monotonically across its own five runs:

    flight  8.45  8.49  8.50  8.56  8.68     (flat)
    trino   7.66  7.82  8.93  9.17 10.50     (+37%)

A later fresh warmup of the same cell ran 6.57 s — faster than any of the five measured
runs — so the 8.37 s median describes whatever was accumulating, not Trino's J2.

J0's order effect was small (flight run 1 at 8.17 s, then 7.93–7.95; the block's warmup
absorbed it) but it is re-measured too, so both scenarios come from one counter-balanced
block rather than one clean and one not.

J1 is NOT here: it was re-measured warm after the oracle fix, both arms flat and tight.
