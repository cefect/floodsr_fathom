""" project configuration parameters. 
not meant to be changed
these are hard-coded to describe the structure of the project
"""

fathom_scenario_dimensions = {
    "return_period": ["1in5", "1in10", "1in20", "1in50", "1in75", "1in100", "1in200", "1in500", "1in1000"],
    "protection": ["DEFENDED", "UNDEFENDED"],
    "hazard_type": ["FLUVIAL", "PLUVIAL", "COASTAL"],
}