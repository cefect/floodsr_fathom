# ADR 03: Sensor Assertion Workers (`assertions.py`, `assertions_sensors.py`)

 

## general
- assertions should be skipped if __debug__=False, so that they can be used in production code without performance concerns
- use optional or lazy imports so we can use the assertions module in all our environments
- for workflow/rule specific assertions, borrow the rule name in the assertion name for readability. 
 