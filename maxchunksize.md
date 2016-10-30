Let's talk about how to choose a reasonable maximum chunk size. The idea
is to prevent chunks to grow super large (which would make them unusable),
but to still allow the "normal" slicing algorithm to do the job most of
the time.

So how is it achieved?

Suppose a random input stream. We want to cut this stream when it has been
suspiciously too long without meeting a window that satisfies the chunk
boundary property (all the c lower bits of its checksum are 1). What is
"suspiciously too long"? Let's pick an arbitrary value and say "there is
about 99.99% chance that a chunk boundary should have been encountered
already".

On each new byte entering the rolling window, there is a probability p
that this byte might be a chunk boundary (the input stream is random, so
new bytes are independent events). And we already know that p=1/2^c,
because all checksums are equally probable and "all the c lower bits are
equal to 1" is exactly one possibility among all the others combinations
of bit values, and there are 2^c of those. Let's proceed to note p=1/2^c.

So now we wonder: Since each entering byte has a probability p of being a
boundary, at some point we should have read enough bytes so that the
probability of having seen a boundary exceeds X, where X is something like
0.999 or 0.9999. But when is that exactly?

Let's note P(B=n) the probability that we meet our next boundary on the
window n:

    P(B=1)=p
    P(B=2)=p(1-p)       (no boundary on 1, a boundary on 2)
    P(B=3)=p(1-p)(1-p)  (no boundary on 1 and 2, a boundary on 3)
    ...
    P(B=n)=p(1-p)^(n-1) (no boundary on 1..(n-1), a boundary on n)

We are interested in the event B<=N, which is the combination of
non-intersecting events B=1 or B=2 or ... or B=n.

    P(B<=N)=Sum(p(1-p)^(n-1), n between 1 and N)
           =p Sum((1-p)^(n-1), n between 1 and N)
    
    we recognize a geometric sum with a ratio (1-p):
    
           = p (1-(1-p)^N)/(1-(1-p))
           = 1-(1-p)^N

So we are looking for N the number of windows to visit such that

          P(B<=N)>=X
    <=>   1-(1-p)^N >= X
    <=>   -(1-p)^N >= X-1
    <=>   (1-p)^N <= 1-X
    <=>   N ln(1-p) <= ln(1-X)
    <=>   N >= ln(1-X)/ln(1-p)

At that point it is interesting to remember p=1/2^c, and thus that 1/p=2^c
is an integer, and that this integer is the average chunk size. Let's have
N=k/p, so that we can express it as a multiple of the average chunk size:

    k/p >= ln(1-X)/ln(1-p)
    k >= ln(1-X) p/ln(1-p)

The negative quantity p/ln(1-p) makes this formula really interesting,
because if one plots it, they realize that it belongs to the interval
]-1,0[. It can actually be proven with Taylor expansions that the limits
of this function are actually -1 and 0 when p->0 and p->1.

    For us, it is good enough to have
    -1 <= p/ln(1-p)
    
    Because it implies
    -ln(1-X) >= ln(1-X) p/ln(1-p)
    
    Therefore, if k => -ln(1-X), then k>=ln(1-X) p/ln(1-p)

So if we choose k >= -ln(1-X) we have our guarantee.

With X=0.999, we would need k>=7, and with X=0.9999, we would need k>=9.
Since we are computer scientists and we love powers of 2, let's pick k=8.

So there we go: If we cut after 8 times the length of the average slice,
we have more than 99.9% chance to have seen a boundary already.
