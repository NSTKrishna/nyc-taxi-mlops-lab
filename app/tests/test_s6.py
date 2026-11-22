"""S6: the prompt keeps a fixed, cacheable prefix, and retrieval asks pgvector for TOP_K by cosine distance."""

import asyncio

import rideapp.assistant as a

CHUNKS = [
    a.Chunk(id="c1", page_id="harbor-line", title="Harbor Line surcharge", content="Harbor Line > Pricing\n\nIt is $4.50.", score=0.84),
    a.Chunk(id="c2", page_id="airport-rules", title="Airport pickups", content="Airport pickups > JFK\n\nUse lot B.", score=0.61),
]


def test_system_prompt_is_the_same_whatever_was_retrieved():
    with_wiki = a.build_messages("How much is the Harbor Line surcharge?", CHUNKS)
    without = a.build_messages("Something else entirely?", [])
    assert with_wiki[0] == without[0] == {"role": "system", "content": a.SYSTEM_PROMPT}  # the cached prefix
    assert [m["role"] for m in with_wiki] == ["system", "user"]


def test_excerpts_go_in_the_user_turn_best_first_then_the_question():
    user = a.build_messages("How much is the Harbor Line surcharge?", CHUNKS)[1]["content"]
    assert user.index("[1] From \"Harbor Line surcharge\" (harbor-line)") < user.index("[2] From \"Airport pickups\"")
    assert "It is $4.50." in user and user.rstrip().endswith("Rider's question: How much is the Harbor Line surcharge?")
    assert "$4.50" not in a.SYSTEM_PROMPT


def test_no_excerpts_says_so():
    user = a.build_messages("What is the pet policy?", [])[1]["content"]
    assert "none found" in user and user.endswith("What is the pet policy?")


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    async def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows, seen):
        self.rows, self.seen = rows, seen

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        self.seen.append((sql, params))
        return FakeCursor(self.rows)


def test_retrieve_asks_for_top_k_by_cosine_distance(monkeypatch):
    seen = []

    async def connect():
        return FakeConn([("c1", "harbor-line", "Harbor Line surcharge", "text", 0.84)], seen)

    monkeypatch.setattr(a, "connect", connect)
    monkeypatch.setattr(a, "TOP_K", 4)
    got = asyncio.run(a.retrieve([0.5, -0.25]))
    assert got == [a.Chunk(id="c1", page_id="harbor-line", title="Harbor Line surcharge", content="text", score=0.84)]
    sql, params = seen[0]
    assert "<=>" in sql and "order by c.embedding <=>" in sql and "limit %(k)s" in sql
    assert params == {"q": "[0.5,-0.25]", "k": 4}  # the vector is a parameter, never formatted into the SQL


def test_undecided_top_k_retrieves_nothing(monkeypatch):
    async def connect():
        raise AssertionError("no database call while TOP_K is undecided")

    monkeypatch.setattr(a, "connect", connect)
    monkeypatch.setattr(a, "TOP_K", 0)
    assert asyncio.run(a.retrieve([0.1])) == []
