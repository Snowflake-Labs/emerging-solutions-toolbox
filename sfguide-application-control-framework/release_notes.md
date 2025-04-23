Release Notes:

Release 1.7
Key features:
- Trust Center Integration
    - Native app providers now have the ability to enforce security requirements for consumer accounts, using the consumer's Trust Center `FINDINGS` Snowflake Account Usage view, before allowing access to their native app.
    - Providers can choose whether to use Trust Center to control access to their app and which Scanners to use, via the App Control Manager Streamlit UI.
- Native Apps Events Sharing Updates
    - The Snowflake Events API now allows native app providers the ability to specify the level of events the consumer is required to share with the provider.
- Sidecar Removal
    - With the updates to native app event sharing, Sidecar is no longer needed, and has been removed.
- Migration to Dynamic Tables
    - Where possible, the ACF will leverage Dynamic Tables, instead of Streams/Tasks.
- Installer Notebooks
    - The ACF now includes a set of notebooks that streamline the "out-of-the-box" ACF deployment process.
    
Improvements:
- Sidecar removal reduces the amount of setup steps the consumer has to execute to configure the native app.
- Installer Notebooks allows developers to easily deploy the latest ACF, without using SnowSQL or executing several scripts in Snowsight Worksheets.

Release 1.6
Key features:
- DEV and PROD environment support
    - The App Control Manager now supports the ability to develop native app logic in a dedicated DEV environment.
    - Once the native app built from the DEV environment passes QA, it can be promoted to the PROD environment.  Any Marketplace-facing app will be built from the PROD environment.
    - This allows native app development on multiple components of a native app (i.e. application logic, Steamlit, etc.) to be developed/tested in parallel.
- Multi-app support for event accounts
    - Event account setup now allows an event account to be used for multiple ACF-created apps.
- Repo reorg
    - The ACF repo has been reorganized to allow for easier deployment of the ACF.
    - The demo app that's included can now be deployed optionally.

Release 1.5
Key features:
- PWS-634: Create an SiS UI for the sample app created in the framework
    - The framework now includes a sample Streamlit UI that allows providers to use the demo app.  This illustrates how to build an SiS UI for the native app.
- PWS-646: Replace consumer data share with Snowflake event table.
    - The consumer no longer has to create data shares.  The consumer simply creates an event table and shares events with the provider.  The native app writes logs/metrics to the event table.
    - The provider creates an event account in each region where their app resides to collect events from consumers in that region.
    - The log signing and verification processes are now removed, since the consumer cannot alter entries in the event table.
- Removed the "Releases" functionality from the App Control Manager (this is available via Snowsight)
- Extend app modes to include:
    - Free:  A free, limited version of the app.
    - Paid:  A paid, limited version of the app where all consumers have the same access and limits.
    - Enterprise: A paid, custom version of the app, where each consumer can have different access and limits.
- App versions now support app modes and the ability to control whether limits are enforced for each version (useful for testing)

Release 1.4.20231009
Key Features:
- PWS-637: Allow for "freemium" app mode
    - Consumers can now use apps created by the ACF directly from the Marketplace, without having to be onboarded first.
    - This allows providers to get a free/limited version of their app to a large audience, without having to manage potential consumers
    - Once potentital consumers are interested in the full version, the provider simply onboards them.
- PWS-635: Integrate Sidecar into the ACF's sample app UI
    - With Sidecar integrated, the amount of setup the consumer has to perform to use the app is greatly reduced to creating and calling one small stored procedure
- PWS-638: Track requests/records processed locally in Consumer's installed app
    - Requests and records processed are now being tracked and limits enforced locally
    - This reduces the reliance on the shared logs, as we phase out the log shareback model.

Bug fixes:
- PWS-633: Update RESET_COUNTER_TASK in the ENABLE_CONSUMER stored procedure
    - The task failed, resulting in records/requests counters not being reset properly.

Maintenance:
- Removed unused ACCEPT_TERMS stored procedure and marketplace controls.

Release 1.3.20230829
Bug fixes:
- PWS-627: Update Streamlit code to reference full file paths
- Fixed typo in COUNTER_RESET_TASK created when ENABLE_CONSUMER is called.

Maintenance:
- PWS-625: Add git PR template to repo

Release 1.3
Key Features:
- Improved handling for logging complex strings
- Added scripts to create provider's dev environment database that allows the provider to test their application logic before building the app
- Added a TEST_CONSUMER that allows the provider to test their application logic
- Added field to ALL_PROCS app table that contains a flag indicating whether the stored procedure requires an input table.  Updated REQUEST to check the table.
    - This is used to prevent consumers avoiding record limit validation
- Renamed P_<APP_CODE>_APP_DB to P_<APP_CODE>_ACF_DB

Release 1.2
Key Features:
- Streamlit UI!!!
    - The ACF now has a Streamlit UI (Application Control Manager) to manage app packages/version, and manage consumers of apps built on the framework
- Repo reorganization, due to Streamlit files
- Added allowed_funcs to METADATA_DICTIONARY table
    - This allows the Provider to specify which functions built into the app, that the Consumer can use
- Native App v2.0 PuPr updates:
    - Created register_single_callback stored procedure in the native app setup_script that allows Consumers to associate objects with native app references (i.e. enrichment)
    - Updated manifest file to add enrichment reference and reference to register_single_callback stored procedure


Release 1.1
Key Features:
- Custom Rules Validation
    - Providers can now introduce custom rules to validate access to their stored procs
    - Rules are stored in METADATA.RULES_DICTIONARY
    - new custom_rules key added to METADATA
- Created METADATA_DICTIONARY table
    - this table stores default and custom metadata keys, along with additional attributes such as default values
- Improved ONBOARD_CONSUMER
    - The stored proc is more flexible and creates consumer metadata using the METADATA_DICTIONARY table
    - The stored proc can override any of the default values by passing them via the PARAMETERS parameter.

Improvements:
- Moved all consumer metadata creation to ONBOARD_CONSUMER (using the METADATA_DICTIONARY table)
    - ENABLE_CONSUMER now only creates applicable Consumer objects and enables the Consumer.
- Removed redundant update statements in VERIFY_METRICS
- Improved RESET_COUNTER_TASK task.  Now the task runs every hour to check if the requests/records processed counters should be reset, based on the interval
- appropriately renamed metadata keys, as needed
- Improved naming convention for ACF objects
    - Using an "app code" for the ACF objects.  This resulted in reducing the number of config parameters for both Provider and Consumer setup.

Release 1.0.20230503
Key Features:
- Refactored the logs/metrics message format and verification processes
- Renamed APP_LOG to APP_LOGGER
    - Metrics collection can now be done using APP_LOGGER
- All metadata keys are now added during ONBOARD_CONSUMER call
    - ENABLE_CONSUMER only updates the 'enabled' value.
- Updated DATABASE ROLES to APPLICATION ROLES
- Moved APP_KEY table to a separate non-versioned schema in setup_script.sql file.

Release 1.0
Key Features:
- Refactored the Identity framework to allow any application logic to be integrated into thia framework.
    - All of the features from the Identity framework are incorporated into this framework
- Native Applications v2 support

Release 1.0.20230503
Key Features:
- Refactored the logs/metrics message format and verification processes
- Renamed APP_LOG to APP_LOGGER
    - Metrics collection can now be done using APP_LOGGER
- All metadata keys are now added during ONBOARD_CONSUMER call
    - ENABLE_CONSUMER only updates the 'enabled' value.
- Updated DATABASE ROLES to APPLICATION ROLES
- Moved APP_KEY table to a separate non-versioned schema in setup_script.sql file.
