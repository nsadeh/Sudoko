# Sudoko-system
Sudoko-system

A system to check the configuration of a Sudoko board and decide whether it is valid or not

## Running instructions

1. Have an installed JDK (8 or 11)
2. Install SBT (https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Mac.html)
3. In the project root, run `sbt`.
4. In the CLI that happens as a result, you can `run valid` or `run tooFewLines`. This will become better soon.


## Future work

By creating an entity type for each configuration, I will be able to process all test cases asynchronously. This requires a few changes to how the program keeps state, but is not difficult.

