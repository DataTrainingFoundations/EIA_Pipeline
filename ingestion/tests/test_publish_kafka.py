from src.publish_kafka import publish_events


class FakeFuture:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.got = False

    def get(self, timeout):  # noqa: ANN001
        self.got = True
        if self.should_fail:
            raise RuntimeError("send failed")
        return None


class FakeProducer:
    def __init__(self, *, fail_on_send: bool = False) -> None:
        self.sent: list[tuple[str, str, dict]] = []
        self.flushed = False
        self.closed = False
        self.fail_on_send = fail_on_send
        self.futures: list[FakeFuture] = []

    def send(self, topic, key, value):  # noqa: ANN001
        self.sent.append((topic, key, value))
        future = FakeFuture(self.fail_on_send)
        self.futures.append(future)
        return future

    def flush(self) -> None:
        self.flushed = True

    def close(self) -> None:
        self.closed = True


class FakeAdminClient:
    def __init__(self, topics: list[str] | None = None) -> None:
        self.topics = set(topics or [])
        self.created: list[str] = []
        self.closed = False

    def list_topics(self):  # noqa: ANN201
        return list(self.topics)

    def create_topics(self, topics):  # noqa: ANN001, ANN201
        for topic in topics:
            self.topics.add(topic.name)
            self.created.append(topic.name)

    def close(self) -> None:
        self.closed = True


def test_publish_events_uses_supplied_producer() -> None:
    producer = FakeProducer()
    admin_client = FakeAdminClient(topics=["eia_topic"])
    count = publish_events(
        topic="eia_topic",
        events=[{"event_id": "1"}, {"event_id": "2"}],
        producer=producer,  # type: ignore[arg-type]
        admin_client=admin_client,  # type: ignore[arg-type]
    )
    assert count == 2
    assert producer.flushed is True
    assert producer.closed is False
    assert producer.sent[0][0] == "eia_topic"
    assert producer.sent[0][1] == "1"
    assert producer.futures[0].got is True
    assert admin_client.created == []


def test_publish_events_raises_when_send_fails() -> None:
    producer = FakeProducer(fail_on_send=True)
    admin_client = FakeAdminClient(topics=["eia_topic"])
    try:
        publish_events(
            topic="eia_topic",
            events=[{"event_id": "1"}],
            producer=producer,  # type: ignore[arg-type]
            admin_client=admin_client,  # type: ignore[arg-type]
        )
    except RuntimeError as exc:
        assert str(exc) == "send failed"
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("publish_events should propagate send failures")


def test_publish_events_creates_missing_topic_even_for_empty_batch() -> None:
    producer = FakeProducer()
    admin_client = FakeAdminClient()

    count = publish_events(
        topic="eia_power",
        events=[],
        producer=producer,  # type: ignore[arg-type]
        admin_client=admin_client,  # type: ignore[arg-type]
    )

    assert count == 0
    assert admin_client.created == ["eia_power"]
    assert producer.sent == []
    assert producer.flushed is True
