# Kafka Large Match Payload Question

## Question for the API team

Hi, I want to confirm how the API handles large match payloads received through
Kafka. API Transform may divide a large document across multiple Kafka
messages. The first message uses `op: "c"`, and later messages use `op: "u"`.

When the API receives an `op: "u"` message, does it append or merge the incoming
sentences and their matches with the existing data, or does it replace the
existing sentence-match list?

We need append or merge behavior so matches received in earlier messages are
not lost. What field does the API use to identify and deduplicate the same
sentence: `globalId` or another identifier?

If the same sentence is received more than once with different portions of its
match list, does the API merge the new matches into that sentence or replace
the matches already saved for it?

## Behavior to confirm

- `op: "c"` creates the initial document.
- `op: "u"` adds later sentence data without removing data received earlier.
- Repeated sentences are identified using a stable identifier.
- Repeated matches are deduplicated instead of being saved twice.
- A retry of the same Kafka message does not duplicate sentences or matches.
