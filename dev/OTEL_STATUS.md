AIP-49: OpenTelementry Support
------------------------------

[Link to AIP](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-49+OpenTelemetry+Support+for+Apache+Airflow)

Status
======
Work in progress and considered experimental.

Updates
=======

- 7 Mar 2023
  - **What Works**
    - There is a Breeze Integration which configures the Breeze/Airflow -> OtelCollector -> Prometheus -> Grafana
pipeline
      - This can be used/tested using `breeze start-airflow --inntegration otel`
    - OTel gauges are working end to end; they are emitted and can be seen and inspected in Grafana
  - **In progress**
    - OTel counters and timers to complete feature parity with StatsD support
    - Add support for OTel Spans
    - Possibly add support for OTel Logging
