**Instructions:**

- Follow the steps below and implement the task. Steps are logical extensions and should be done in order.
- Your goal is to build a small working system. Identify the core problem at each step, solve it, and move on.
- Make tradeoffs between reliability, observability, and architecture to deliver the best overall result.
- You are free to use tools of your choice.

**Goal:**

We're going to build a lightweight sandbox orchestration platform. AI agents at our company run inside isolated sandboxes spun up on demand. Your job is to build a small service that manages their lifecycle.

- Step 1

    **The basics**

    Start with a simple job queue. Set up a producer that creates jobs like `{ "jobId": "abc123", "type": "http" }` and a consumer that picks them up and logs the work.

- Step 2

    **Doing something useful**

    Extend the consumer to spin up a container running a simple HTTP server per job and log its URL.

- Step 3

    **Multiple sandbox types**

    Add a `browser` job type that needs a Chrome container with an exposed CDP port. Design so new sandbox types require minimal changes.

- Step 4

    **Observable orchestration**

    Add a view to observe sandboxes as they come and go. We should be able to derive the system's key state at any point.

- Step 5

    **Failure and lifecycle**

    Handle sandbox failures and add a cleanup mechanism so sandboxes don't run forever (TTL, idle timeout, or explicit release).
