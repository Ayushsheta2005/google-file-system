#!/usr/bin/env python3
"""Reed-Solomon tests.

Structured in three layers, because a bug in the field arithmetic produces
garbage that still *looks* like plausible shards:

  1. GF(2^8) field axioms -- the algebra has to hold before anything above
     it can be trusted.
  2. Matrix properties -- in particular that every k x k submatrix inverts,
     which is the property the whole scheme rests on.
  3. Codec round-trips under every recoverable erasure pattern, exhaustively
     for small configurations rather than on a sample.
"""

import itertools
import os
import random
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reedsolomon import (  # noqa: E402
    ANTILOG_TABLE,
    FIELD_SIZE,
    LOG_TABLE,
    ReedSolomon,
    gf_add,
    gf_div,
    gf_inv,
    gf_mul,
    gf_mul_vec,
    gf_pow,
    matrix_inverse,
    systematic_matrix,
    vandermonde,
)


# ===================================================================
# 1. The field
# ===================================================================
def test_generator_visits_every_nonzero_element():
    """If g=3 were not primitive its powers would cycle early, the log table
    would not be a bijection, and every multiplication above would be wrong
    in a way that still produces bytes."""
    seen = {int(ANTILOG_TABLE[i]) for i in range(255)}
    assert seen == set(range(1, 256))


def test_log_and_antilog_are_inverses():
    for a in range(1, FIELD_SIZE):
        assert int(ANTILOG_TABLE[int(LOG_TABLE[a])]) == a


def test_addition_is_xor_and_is_its_own_inverse():
    for a in range(0, 256, 7):
        for b in range(0, 256, 11):
            assert gf_add(a, b) == a ^ b
            assert gf_add(gf_add(a, b), b) == a


def test_multiplication_identity_and_zero():
    for a in range(FIELD_SIZE):
        assert gf_mul(a, 1) == a
        assert gf_mul(a, 0) == 0


def test_multiplication_is_commutative():
    for a in range(0, 256, 5):
        for b in range(0, 256, 7):
            assert gf_mul(a, b) == gf_mul(b, a)


def test_multiplication_is_associative():
    rng = random.Random(1234)
    for _ in range(400):
        a, b, c = (rng.randrange(256) for _ in range(3))
        assert gf_mul(gf_mul(a, b), c) == gf_mul(a, gf_mul(b, c))


def test_multiplication_distributes_over_addition():
    rng = random.Random(99)
    for _ in range(400):
        a, b, c = (rng.randrange(256) for _ in range(3))
        assert gf_mul(a, gf_add(b, c)) == gf_add(gf_mul(a, b), gf_mul(a, c))


def test_every_nonzero_element_has_an_inverse():
    for a in range(1, FIELD_SIZE):
        assert gf_mul(a, gf_inv(a)) == 1


def test_zero_has_no_inverse():
    with pytest.raises(ZeroDivisionError):
        gf_inv(0)
    with pytest.raises(ZeroDivisionError):
        gf_div(5, 0)


def test_division_undoes_multiplication():
    rng = random.Random(7)
    for _ in range(400):
        a = rng.randrange(256)
        b = rng.randrange(1, 256)
        assert gf_div(gf_mul(a, b), b) == a


def test_pow_matches_repeated_multiplication():
    for a in (0, 1, 2, 3, 17, 200, 255):
        acc = 1
        for n in range(8):
            assert gf_pow(a, n) == acc
            acc = gf_mul(acc, a)


def test_vectorized_multiply_agrees_with_scalar():
    """The data path uses the vectorized version exclusively; if it diverges
    from the scalar one, parity is wrong and nothing says so."""
    rng = np.random.default_rng(42)
    vec = rng.integers(0, 256, size=1000, dtype=np.uint8)
    for scalar in (0, 1, 2, 7, 128, 255):
        expected = np.array([gf_mul(scalar, int(v)) for v in vec], dtype=np.uint8)
        assert np.array_equal(gf_mul_vec(scalar, vec), expected)


# ===================================================================
# 2. The matrices
# ===================================================================
def test_matrix_times_its_inverse_is_identity():
    rng = random.Random(5)
    for n in (2, 3, 6):
        for _ in range(20):
            m = np.array([[rng.randrange(256) for _ in range(n)]
                          for _ in range(n)], dtype=np.uint8)
            try:
                inv = matrix_inverse(m)
            except ValueError:
                continue       # singular; fine
            from reedsolomon import gf_matmul
            assert np.array_equal(gf_matmul(m, inv), np.eye(n, dtype=np.uint8))


def test_singular_matrix_is_rejected():
    m = np.array([[1, 2], [2, 4]], dtype=np.uint8)   # row2 = 2*row1 in GF
    with pytest.raises(ValueError):
        matrix_inverse(m)


def test_systematic_matrix_has_identity_on_top():
    """Without this a read that lost nothing would still pay a full decode,
    and nothing would visibly break -- only the benchmark would notice."""
    for k, m in ((4, 2), (6, 3), (10, 4)):
        mat = systematic_matrix(k, m)
        assert mat.shape == (k + m, k)
        assert np.array_equal(mat[:k], np.eye(k, dtype=np.uint8))


def test_every_k_by_k_submatrix_is_invertible():
    """THE property the scheme rests on: whichever k shards survive, the
    corresponding rows must form a solvable system. Checked exhaustively
    over all C(9,6)=84 combinations for RS(6,3)."""
    k, m = 6, 3
    mat = systematic_matrix(k, m)
    for rows in itertools.combinations(range(k + m), k):
        matrix_inverse(mat[list(rows)])     # must not raise


def test_vandermonde_submatrices_are_invertible():
    v = vandermonde(7, 4)
    for rows in itertools.combinations(range(7), 4):
        matrix_inverse(v[list(rows)])


# ===================================================================
# 3. The codec
# ===================================================================
@pytest.fixture
def rs():
    return ReedSolomon(6, 3)


def test_encode_produces_n_equal_sized_shards(rs):
    shards, length = rs.encode(b'hello world' * 100)
    assert len(shards) == 9
    assert len({len(s) for s in shards}) == 1
    assert length == 1100


def test_systematic_shards_are_the_input_verbatim(rs):
    data = bytes(range(256)) * 4
    shards, _ = rs.encode(data)
    assert b''.join(shards[:rs.k])[:len(data)] == data


def test_roundtrip_with_nothing_lost(rs):
    data = os.urandom(10000)
    shards, length = rs.encode(data)
    assert rs.decode(list(shards), length) == data


def test_roundtrip_losing_every_single_shard_in_turn(rs):
    data = os.urandom(5000)
    shards, length = rs.encode(data)
    for i in range(rs.n):
        damaged = list(shards)
        damaged[i] = None
        assert rs.decode(damaged, length) == data, f'failed losing shard {i}'


def test_roundtrip_losing_every_pair_of_shards(rs):
    data = os.urandom(3000)
    shards, length = rs.encode(data)
    for combo in itertools.combinations(range(rs.n), 2):
        damaged = list(shards)
        for i in combo:
            damaged[i] = None
        assert rs.decode(damaged, length) == data, f'failed losing {combo}'


def test_roundtrip_losing_every_triple_the_maximum_tolerated(rs):
    """RS(6,3) survives exactly 3 losses. All C(9,3)=84 patterns, because a
    scheme that works for most patterns and not one of them is worthless."""
    data = os.urandom(2000)
    shards, length = rs.encode(data)
    for combo in itertools.combinations(range(rs.n), 3):
        damaged = list(shards)
        for i in combo:
            damaged[i] = None
        assert rs.decode(damaged, length) == data, f'failed losing {combo}'


def test_losing_more_than_m_shards_is_refused_not_guessed(rs):
    """Must fail loudly. Silently returning corrupt data here would be the
    worst possible behaviour for a storage system."""
    data = os.urandom(2000)
    shards, length = rs.encode(data)
    damaged = list(shards)
    for i in range(4):
        damaged[i] = None
    with pytest.raises(ValueError):
        rs.decode(damaged, length)


def test_tolerates_more_failures_than_triple_replication(rs):
    """The headline claim: RS(6,3) survives 3 losses; 3x replication does
    not survive its 3rd."""
    assert rs.m == 3
    data = os.urandom(1000)
    shards, length = rs.encode(data)
    damaged = list(shards)
    for i in (0, 4, 8):
        damaged[i] = None
    assert rs.decode(damaged, length) == data


def test_storage_overhead_is_one_and_a_half(rs):
    assert rs.storage_overhead == pytest.approx(1.5)
    assert ReedSolomon(10, 4).storage_overhead == pytest.approx(1.4)


# ------------------------------------------------------------ edge cases
def test_empty_input():
    rs = ReedSolomon(4, 2)
    shards, length = rs.encode(b'')
    assert length == 0
    assert rs.decode(list(shards), 0) == b''


def test_input_shorter_than_one_shard():
    rs = ReedSolomon(6, 3)
    data = b'ab'
    shards, length = rs.encode(data)
    damaged = list(shards)
    damaged[0] = damaged[7] = None
    assert rs.decode(damaged, length) == data


def test_padding_is_stripped_on_decode():
    """Shards are equal-length, so short input is zero-padded. Losing the
    original length makes the decoded file silently longer."""
    rs = ReedSolomon(6, 3)
    for n in (1, 5, 7, 13, 100, 1001):
        data = os.urandom(n)
        shards, length = rs.encode(data)
        assert rs.decode(list(shards), length) == data
        assert len(rs.decode(list(shards), length)) == n


def test_all_zero_and_all_ff_inputs():
    """Degenerate inputs that make a broken log table look fine."""
    rs = ReedSolomon(4, 2)
    for data in (b'\x00' * 1000, b'\xff' * 1000):
        shards, length = rs.encode(data)
        damaged = list(shards)
        damaged[0] = damaged[5] = None
        assert rs.decode(damaged, length) == data


@pytest.mark.parametrize('k,m', [(2, 1), (3, 2), (4, 2), (6, 3), (10, 4), (8, 8)])
def test_other_configurations_round_trip(k, m):
    rs = ReedSolomon(k, m)
    data = os.urandom(777)
    shards, length = rs.encode(data)
    damaged = list(shards)
    for i in random.Random(k * 100 + m).sample(range(k + m), m):
        damaged[i] = None
    assert rs.decode(damaged, length) == data


def test_rejects_impossible_configurations():
    with pytest.raises(ValueError):
        ReedSolomon(0, 3)
    with pytest.raises(ValueError):
        ReedSolomon(6, 0)
    with pytest.raises(ValueError):
        ReedSolomon(200, 100)      # k+m > 255 exceeds the field


# ------------------------------------------------------- single-shard repair
def test_reconstruct_one_shard_without_rebuilding_everything(rs):
    """Repairing a lost replica should not require materialising the whole
    object -- this is the operation ADR-005's shard repair uses."""
    data = os.urandom(4000)
    shards, _ = rs.encode(data)
    for lost in range(rs.n):
        damaged = list(shards)
        damaged[lost] = None
        assert rs.reconstruct_shard(damaged, lost) == shards[lost]


def test_reconstruct_returns_an_intact_shard_unchanged(rs):
    shards, _ = rs.encode(os.urandom(1000))
    assert rs.reconstruct_shard(list(shards), 2) == shards[2]


def test_fast_multiply_matches_schoolbook_for_all_65536_pairs():
    """Exhaustive. The log-table path is an optimisation of _gf_mul_slow and
    must agree with it everywhere -- a disagreement on even one pair would
    corrupt exactly the shards that happen to use that coefficient."""
    from reedsolomon import _gf_mul_slow
    for a in range(256):
        for b in range(256):
            assert gf_mul(a, b) == _gf_mul_slow(a, b), f'mismatch at {a}*{b}'


def test_build_tables_rejects_a_non_primitive_generator(monkeypatch):
    """The guard that would have caught the original bug: 3 has order 51
    under 0x11D, so the table build must refuse rather than produce a log
    table covering a fifth of the field."""
    import reedsolomon as rsmod
    monkeypatch.setattr(rsmod, 'GENERATOR', 3)
    with pytest.raises(RuntimeError, match='not primitive'):
        rsmod._build_tables()


def test_the_order_51_case_is_specifically_caught():
    """Regression test for a weakness in the first version of the guard.

    An element's order always divides 255, so 3 (order 51 under 0x11D) also
    returns to 1 after 255 steps -- the original 'did we get back to 1?'
    check passed it. Only a distinctness check catches it.
    """
    import reedsolomon as rsmod
    x, seen = 1, set()
    for _ in range(255):
        seen.add(x)
        x = rsmod._gf_mul_slow(x, 3)
    assert x == 1, 'order of 3 should still divide 255'
    assert len(seen) == 51, 'and yet it covers only 51 elements'
