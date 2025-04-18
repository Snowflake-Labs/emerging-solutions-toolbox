# Serverless Task Migration

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

The Serverless task migration app is a tool for analyzing the running tasks on your snowflake account and migrating anything that may not be optimized.
The app looks at two siatuations in particular

- Tasks that run quickly, and spin up a warehouse but do not utilize enough time on it to justify the cost of starting it.
- Tasks that are long running and regularly overshoot their next target start schedule

The goal is to migrate any tasks in either situation to a serverless task, this is due to two of the features in a serverless task. The fact that it is serverless and thus does not spin up a warehouse, and the fact that they are autoscaling and adjust their warehouse size based on the schedule to keep the task running efficiently.

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is
provided `as is` and without warranty. Snowflake will not offer any support for the use
of the sample code. The purpose of the code is to provide customers with easy access to
innovative ideas that have been built to accelerate customers' adoption of key
Snowflake features. We certainly look for customers' feedback on these solutions and
will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

## Tagging

Please see `TAGGING.md` for details on object comments.
