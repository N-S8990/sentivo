"""Sample financial texts for testing the sentiment pipeline."""

texts = [
    # Positive
    "The company posted a 15 % earnings beat this quarter, driven by strong demand across all segments.",
    "Phase 3 trial results exceeded expectations, sending the stock up 12% in after-hours trading.",
    "Cost-cutting measures widened margins significantly, boosting investor confidence.",
    "Record pre-order numbers for the new product reinforce the company's market leadership.",
    "A multi-billion dollar government contract secures revenue visibility for the next five years.",
    # Negative
    "Supply chain disruptions forced the automaker to slash production targets for the second half.",
    "The CEO's sudden resignation triggered a sharp sell-off amid leadership uncertainty.",
    "Widespread recalls over safety defects have eroded consumer trust and market share.",
    "Rising input costs and fierce competition squeezed margins to a decade low.",
    "Regulatory delays have indefinitely stalled the flagship infrastructure project.",
    # Neutral / Mixed
    "The central bank held rates steady, citing conflicting signals on inflation and growth.",
    "Revenue grew modestly in line with estimates, but higher R&D spend weighed on net income.",
    "Analysts are divided on the stock — strong user growth tempered by monetisation concerns.",
    "Trade negotiations remain unresolved, keeping currency markets volatile.",
    "Retail performance was mixed: discount chains thrived while luxury brands softened.",
] * 20
