# Solution Installation Wizard

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

The Solution Installation Wizard facilitates the deployment of code products (native apps, Streamlits, and more) securely and safely into consumer environments with full consumer consent.  It is currently designed to be used by known consumers.

All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

Please see TAGGING.md for details on object comments.

## Definitions
**Provider** - The entity providing the Solution Installation Wizard app

**Consumer** - The entity installing and using the Solution Installation Wizard to create Apps, most likely to share back to the provider

**Placeholder** - A string that is detected and replaced when the consumer uses the app.  This is useful if you have consumer-specific names for objects, like databases, shares, apps, etc..  It lets you personalize the scripts to each consumer.  **Note** Placeholders are defined per consumer, so a consumer must be known in order to use placeholders.

**Script** - A SQL script, meant to faciliate the deployment of some code into the consumer environment - that code can be a native app, a streamlit, a share, or really anything.

**Workflow** - A set of SQL scripts, useful for adding a menu of solutions to the app.

## Provider Directions
-- Initial Setup --
1. Run 1-sol_inst_wzd_provider_setup.sql
2. Load your own scripts and consumer-specific placeholders -- OR -- run 2-demo_configuration.sql to load sample configurations
3. Run 3-sol_inst_wzd_package_setup.sql

**Note** Step 3 automatically installs the app locally, so you can test it within the provider account

## Customization Options
**Custom Placeholders (Optional)**
If you need the scripts to dynamically swap out text based on which consumer is using the app, you can add placeholders to *sol_inst_wzd_package.admin.placeholder_definition*.
To do so, you insert the consumer organization, consumer account name, placeholder text (the string to find), and the replacement value (the text that replaces the found string)

**Custom scripts**
To load custom scripts, add them to *sol_inst_wzd_package.admin.script*.
To do so, you insert the script by including:
 - *workflow name* - what the collection of scripts deploy
 - *workflow description* - a human readable description of what the scripts deploy
 - *script name* - unique name of the script
 - *script order* - each script's step number in the workflow, should be unique, integer, and continuously incrementing
 - *is autorun* - whether or not the script is run directly by the app - an app has more limited permissions compared to a consumer, so this depends on the script
 - *is autorun code visible* - whether or not the code that will be automatically run is exposed to the consumer
 - *script description* - a human readable description of what the script does
 - *script text* - the actual content of the scripts - **Note** if using placeholders, make sure the original placeholder exists in the script

 If your script has $$ characters in it, you may run into issues inserting.  To get around that, you can use a REGEXP_REPLACE method, using ::: instead of $$ in the script.  *Streamlit Deployment* provides an example.

**Native App Manifest**
The native app manifest can be updated in 3-sol_inst_wzd__package_setup to change the installed name of the app - name, label, and comment are freely changeable.

**Native App Readme**
The native app readme can be changed in 3-sol_inst_wzd__package_setup.

**Listing**
When listing the app, you can call the app whatever fits your use case, it does not have to match the package name.

## Consumer Directions
1. Install the App Deployer from the listing
2. Open the app in Snowsight via the sidebar - Data Products -> Apps (this should automatically open the Streamlit)
3. Follow the on-screen directions in the Streamlit to select and deploy code
