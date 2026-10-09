#!/usr/bin/env python3
"""Reed-Solomon erasure coding over GF(2^8), vectorized with numpy.

Why this exists: 3x replication costs 3x storage. Reed-Solomon RS(k, m) splits
data into k shards and computes m parity shards, and **any k of the k+m shards
reconstruct the original**. RS(6,3) therefore survives 3 simultaneous losses --
strictly better than 3x replication, which dies on the 3rd -- at 1.5x storage
instead of 3x. That is why Google retired GFS for Colossus. See ADR-005.

----------------------------------------------------------------------------
The whole idea in one paragraph
----------------------------------------------------------------------------
Treat each byte position independently. The k data shards give you a column
vector d of k bytes. Multiply by an (k+m) x k encoding matrix A to get k+m
output bytes. Lose any m of them; cross out those rows of A, leaving a k x k
matrix A'. Because A is built so that *every* k x k submatrix is invertible,
A' can be inverted, and d = A'^-1 * (surviving bytes). Encoding is a matrix
multiply; decoding is a matrix inverse plus a matrix multiply.

The catch: ordinary integer arithmetic overflows a byte. So all of it is done
in the finite field GF(2^8), where add, multiply and divide are defined on the
256 possible byte values and always produce a byte.

----------------------------------------------------------------------------
GF(2^8) in practice
----------------------------------------------------------------------------
Elements are bytes. Addition is XOR (so addition and subtraction are the same
operation, and every element is its own negative). Multiplication is
polynomial multiplication modulo the irreducible polynomial 0x11D -- the same
one AES uses.

Doing that per byte in Python would be unusably slow, so multiplication is
done with logarithms. The field has a generator g=3 whose powers cycle through
all 255 non-zero elements, which turns multiply into add-the-logs:

    a * b = antilog[(log[a] + log[b]) mod 255]

and the whole thing becomes table lookups that numpy can apply to a megabyte
at a time.

----------------------------------------------------------------------------
Choice of encoding matrix
----------------------------------------------------------------------------
A Vandermonde matrix has the "every square submatrix is invertible" property,
but it is numerically awkward to turn into *systematic* form (where the first
k output shards are the input data verbatim). Systematic form matters a lot
here: when nothing is lost -- the overwhelmingly common case -- reading is
just concatenating the data shards with no decode at all.

So the matrix is built as Vandermonde and then transformed so the top k x k
block is the identity. The construction and the invertibility property are
both checked in the tests rather than taken on faith.
"""

import numpy as np

# x^8 + x^4 + x^3 + x^2 + 1. This is the polynomial Jerasure and most
# Reed-Solomon implementations use. (AES uses a *different* one, 0x11B --
# worth not confusing them, because the choice of polynomial determines which
# elements are primitive and the two disagree.)
PRIMITIVE_POLY = 0x11D

# A primitive element: its successive powers must visit all 255 non-zero
# elements before repeating, which is what makes the log table a bijection.
#
# This pairing is not free to choose. Under 0x11D the element 2 has order 255
# and 3 has order only 51; under 0x11B it is exactly the other way round. Pick
# the wrong pair and the log table silently covers 51 of 256 values, every
# multiplication through it is wrong, and the parity shards look like
# perfectly plausible bytes. `test_generator_visits_every_nonzero_element`
# exists specifically to catch that.
GENERATOR = 2
FIELD_SIZE = 256


def _gf_mul_slow(a: int, b: int) -> int:
    """Schoolbook carry-less multiply with reduction -- used only to build
    the tables, and as the reference the fast path is tested against."""
    product = 0
    for _ in range(8):
        if b & 1:
            product ^= a
        high_bit = a & 0x80
        a = (a << 1) & 0xFF
        if high_bit:
            a ^= PRIMITIVE_POLY & 0xFF
        b >>= 1
    return product


def _build_tables():
    """Discrete log and antilog tables for GF(2^8)."""
    log = np.zeros(FIELD_SIZE, dtype=np.uint8)
    antilog = np.zeros(FIELD_SIZE * 2, dtype=np.uint8)

    x = 1
    for i in range(255):
        antilog[i] = x
        log[x] = i
        x = _gf_mul_slow(x, GENERATOR)

    # Checking that the powers return to 1 is NOT sufficient: the order of an
    # element always divides 255, so a generator of order 51 (which is what 3
    # is under this polynomial) also lands back on 1 after 255 steps and
    # passes that test. The real requirement is that the 255 powers are
    # *distinct* -- i.e. the log table is a bijection on the non-zero field.
    distinct = len(set(int(v) for v in antilog[:255]))
    if distinct != 255:
        raise RuntimeError(
            f'GENERATOR={GENERATOR} is not primitive under '
            f'POLY=0x{PRIMITIVE_POLY:X}: its powers cover only {distinct} of '
            f'255 non-zero elements, so the log table is not a bijection')

    # Duplicate so (log[a] + log[b]), which reaches 508, can index without a
    # modulo on the hot path.
    antilog[255:510] = antilog[0:255]
    return log, antilog


LOG_TABLE, ANTILOG_TABLE = _build_tables()


# --------------------------------------------------------------------------
# Scalar field arithmetic (used to build matrices; not on the data path)
# --------------------------------------------------------------------------
def gf_add(a: int, b: int) -> int:
    """Addition in GF(2^8) is XOR -- and so is subtraction."""
    return a ^ b


def gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return int(ANTILOG_TABLE[int(LOG_TABLE[a]) + int(LOG_TABLE[b])])


def gf_div(a: int, b: int) -> int:
    if b == 0:
        raise ZeroDivisionError('division by zero in GF(2^8)')
    if a == 0:
        return 0
    return int(ANTILOG_TABLE[(int(LOG_TABLE[a]) - int(LOG_TABLE[b])) % 255])


def gf_inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError('zero has no multiplicative inverse')
    return int(ANTILOG_TABLE[(255 - int(LOG_TABLE[a])) % 255])


def gf_pow(a: int, n: int) -> int:
    if a == 0:
        return 0 if n else 1
    return int(ANTILOG_TABLE[(int(LOG_TABLE[a]) * n) % 255])


# --------------------------------------------------------------------------
# Vectorized field arithmetic (the data path)
# --------------------------------------------------------------------------
def gf_mul_vec(scalar: int, vec: np.ndarray) -> np.ndarray:
    """Multiply a whole byte array by one field element.

    This is the inner loop of both encode and decode, so it is written as
    pure table lookups on uint8 arrays: numpy does the whole buffer in C.
    Doing it byte-by-byte in Python costs roughly three orders of magnitude.
    """
    if scalar == 0:
        return np.zeros_like(vec)
    if scalar == 1:
        return vec.copy()
    nonzero = vec != 0
    out = np.zeros_like(vec)
    # int16 because two logs can sum past 255; the duplicated antilog table
    # is what lets us skip the modulo.
    idx = LOG_TABLE[vec[nonzero]].astype(np.int16) + int(LOG_TABLE[scalar])
    out[nonzero] = ANTILOG_TABLE[idx]
    return out


def gf_matmul(matrix: np.ndarray, data: np.ndarray) -> np.ndarray:
    """(rows x k) field matrix times (k x n) byte block -> (rows x n).

    Accumulation is XOR because that is what addition means here.
    """
    rows, k = matrix.shape
    n = data.shape[1]
    out = np.zeros((rows, n), dtype=np.uint8)
    for r in range(rows):
        acc = np.zeros(n, dtype=np.uint8)
        for c in range(k):
            coeff = int(matrix[r, c])
            if coeff:
                acc ^= gf_mul_vec(coeff, data[c])
        out[r] = acc
    return out


# --------------------------------------------------------------------------
# Matrices
# --------------------------------------------------------------------------
def vandermonde(rows: int, cols: int) -> np.ndarray:
    """V[i][j] = i^j in GF(2^8).

    Every square submatrix of a Vandermonde matrix is invertible, which is
    exactly the property erasure coding needs: whichever k shards survive,
    the corresponding k rows form a solvable system.
    """
    m = np.zeros((rows, cols), dtype=np.uint8)
    for i in range(rows):
        for j in range(cols):
            m[i, j] = gf_pow(i, j) if i or j else 1
    return m


def matrix_inverse(m: np.ndarray) -> np.ndarray:
    """Gauss-Jordan elimination in GF(2^8).

    Identical to the real-number algorithm except that subtraction is XOR and
    division uses the field inverse. No pivoting for numerical stability is
    needed -- there is no rounding in a finite field, so the only reason to
    pivot is a zero on the diagonal.
    """
    n = m.shape[0]
    if m.shape[0] != m.shape[1]:
        raise ValueError('only square matrices can be inverted')

    work = m.astype(np.uint8).copy()
    inv = np.eye(n, dtype=np.uint8)

    for col in range(n):
        pivot = col
        while pivot < n and work[pivot, col] == 0:
            pivot += 1
        if pivot == n:
            raise ValueError('matrix is singular')
        if pivot != col:
            work[[col, pivot]] = work[[pivot, col]]
            inv[[col, pivot]] = inv[[pivot, col]]

        scale = gf_inv(int(work[col, col]))
        work[col] = gf_mul_vec(scale, work[col])
        inv[col] = gf_mul_vec(scale, inv[col])

        for row in range(n):
            if row == col:
                continue
            factor = int(work[row, col])
            if factor:
                work[row] ^= gf_mul_vec(factor, work[col])
                inv[row] ^= gf_mul_vec(factor, inv[col])

    return inv


def systematic_matrix(k: int, m: int) -> np.ndarray:
    """(k+m) x k encoding matrix whose top k rows are the identity.

    Systematic means the first k output shards are the input data verbatim,
    so a read that loses nothing is a concatenation and never touches the
    decoder. Built by taking a Vandermonde matrix and multiplying through by
    the inverse of its top k x k block, which preserves the
    every-submatrix-invertible property while forcing the identity on top.
    """
    v = vandermonde(k + m, k)
    top_inv = matrix_inverse(v[:k])
    full = gf_matmul(v, top_inv)
    # Guard against a silent construction error: without the identity on top,
    # every read would pay a decode and nothing would obviously break.
    assert np.array_equal(full[:k], np.eye(k, dtype=np.uint8)), \
        'systematic construction failed'
    return full


# --------------------------------------------------------------------------
# Codec
# --------------------------------------------------------------------------
class ReedSolomon:
    """RS(k, m): k data shards, m parity shards, tolerates m losses.

    Shards are equal length; the original byte count is carried separately so
    trailing zero padding can be stripped on decode. Losing that length makes
    a decoded file silently longer than the original.
    """

    def __init__(self, data_shards: int = 6, parity_shards: int = 3):
        if data_shards < 1 or parity_shards < 1:
            raise ValueError('need at least one data and one parity shard')
        if data_shards + parity_shards > 255:
            raise ValueError(
                'GF(2^8) has only 255 distinct non-zero rows, so k+m must be <= 255')
        self.k = data_shards
        self.m = parity_shards
        self.n = data_shards + parity_shards
        self.matrix = systematic_matrix(self.k, self.m)

    # ------------------------------------------------------------- encode
    def shard_size(self, length: int) -> int:
        return (length + self.k - 1) // self.k

    def encode(self, data: bytes):
        """Split into k data shards and compute m parity shards.

        Returns (shards, original_length). `shards` has n entries, the first
        k being the data verbatim (systematic form).
        """
        if not data:
            return [b''] * self.n, 0

        size = self.shard_size(len(data))
        padded = np.frombuffer(
            data + b'\x00' * (size * self.k - len(data)), dtype=np.uint8)
        grid = padded.reshape(self.k, size)

        parity = gf_matmul(self.matrix[self.k:], grid)
        shards = [grid[i].tobytes() for i in range(self.k)]
        shards += [parity[i].tobytes() for i in range(self.m)]
        return shards, len(data)

    # ------------------------------------------------------------- decode
    def decode(self, shards, original_length: int) -> bytes:
        """Reconstruct from any k present shards.

        `shards` is a list of n entries where a missing shard is None.
        """
        if original_length == 0:
            return b''

        present = [i for i, s in enumerate(shards) if s is not None]
        if len(present) < self.k:
            raise ValueError(
                f'need {self.k} shards to reconstruct, have {len(present)}')

        # Fast path: all data shards intact, so nothing to solve.
        if all(shards[i] is not None for i in range(self.k)):
            return b''.join(shards[:self.k])[:original_length]

        use = present[:self.k]
        size = len(shards[use[0]])

        sub = self.matrix[use]
        inv = matrix_inverse(sub)

        block = np.vstack([np.frombuffer(shards[i], dtype=np.uint8) for i in use])
        recovered = gf_matmul(inv, block)
        return recovered.reshape(-1)[:original_length].tobytes()

    def reconstruct_shard(self, shards, index: int) -> bytes:
        """Rebuild one specific shard, for repairing a single lost replica
        without materialising the whole object."""
        if shards[index] is not None:
            return shards[index]
        present = [i for i, s in enumerate(shards) if s is not None]
        if len(present) < self.k:
            raise ValueError(f'need {self.k} shards, have {len(present)}')

        use = present[:self.k]
        inv = matrix_inverse(self.matrix[use])
        block = np.vstack([np.frombuffer(shards[i], dtype=np.uint8) for i in use])
        data_grid = gf_matmul(inv, block)
        row = gf_matmul(self.matrix[index:index + 1], data_grid)
        return row[0].tobytes()

    # -------------------------------------------------------------- info
    @property
    def storage_overhead(self) -> float:
        """1.5 for RS(6,3) versus 3.0 for triple replication."""
        return self.n / self.k

    def __repr__(self):
        return (f'ReedSolomon(k={self.k}, m={self.m}, '
                f'overhead={self.storage_overhead:.2f}x, tolerates={self.m})')
