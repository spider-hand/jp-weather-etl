# JP Weather ETL

## Test Design

- Always prefer explicit Arrange–Act–Assert tests.
- Always separate the normal case, ties or boundary cases, invalid input, and missing data when they represent different behaviors.
- Do not remove useful scenario context merely to reduce duplication. Keep scenario-specific inputs, queries, actions, and expectations visible in the test.
- Only use parameterization when every case verifies the same behavior and each failure remains easy to understand.
- Always name tests after externally observable behavior. Do not name tests after internal function calls, helper methods, or implementation structure.
- Always make tests represent the current durable contract, not the history of the current session.
- Always ask before adding a test: "Would this test still make sense to a maintainer who does not know the conversation or change history?"
