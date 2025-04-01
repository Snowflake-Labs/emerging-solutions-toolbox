# Supply Chain Network Optimization
The Supply Chain Network Optimization solution provides a method for optimizing decisions through linear programming, utilizing Snowflake, Streamlit, Snowpark, and PuLP.

All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty. Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

Please see TAGGING.md for details on object comments.

## Overview
The Supply Chain Network Optimization Model utilizes [Linear Programming](https://en.wikipedia.org/wiki/Linear_programming), also called linear optimization or constraint programming.

Linear programming uses a system of inequalities to define a feasible regional mathematical space, and a 'solver' that traverses that space by adjusting a number of decision variables, efficiently finding the most optimal set of decisions given constraints to meet a stated objective function.

It is possible to also introduce integer-based decision variables, which transforms the problem from a *linear program* into a *mixed integer program*, but the mechanics are fundamentally the same.

The **objective function** defines the goal - maximizing or minimizing a value, such as profit or costs.
**Decision variables** are a set of decisions - the values that the solver can change to impact the objective value.
The **constraints** define the realities of the business - such as only shipping up to a stated capacity.

We are utilizing the PuLP library, which allows us to define a linear program using Python and use virtually any solver we want.  We are using PuLP's included [CBC solver](https://github.com/coin-or/Cbc) for our models.

## Problem(s) to be Solved
For our model, we are large business with a large network of factories, distributors, and customers.  We want to deliver product in the cheapest possible way.

Each factory has its own set of production capacities, production costs, and freight costs.  Each Distributor has its own set of throughput capacities, throughput costs, and freight costs.  Each customer has a set of demand.  Freight is a cross join between every factory and distributor, and every distributor and customer.  We are assuming that Factories cannot ship directly to Customers in this model - this emulates bulk shipments likely to come out of factories.

Since this is a cost model, we are assuming that all customer demand must be met - so no deciding to not ship to a customer, regardless of cost.

We are solving the problem with three different objectives... emulating a "crawl, walk, run" for reaching truly optimal fulfillment planning.  The problems are:

- **Minimize Distance** (Crawl) - Emulating a fairly naive present-day, where only distance is considered.  This is typically how planning will be broken up if not treated as a system.
- **Minimize Freight** (Walk) - Emulating an organization with freight analytics capability, where only freight cost is considered.  This is typically how planning happens in organizations with a freight planning function.
- **Minimize Total Fulfillment** (Run) - Emulating a fully realized network analytics capability, where all production/throughput/freight costs are considered.  This is what happens when the full business is understood and considered during planning.

## Takeaways
**Minimize Total Fulfillment** presents the most optimal, cheapest fulfillment plan, even if some individual decisions look to not make sense in isolation.  Any deviation from the plan will incur additional cost.  This does *not* mean this model is completely doable in reality, but any attempt to match the business to it will improve costs.

This is also a cost optimization model... it assumes you have to ship every customer their demand, even if it is a money-losing shipment.  To flip it to a "what customers should I ship to?", you would adjust the objective function to a profit maximimization model, and you would need to tie in revenue to it - first by adding price to every current customer, and adding every potential customer you know about and estimates of their volume/price/freight costs.

## Cortex
In addition to making an optimal decision set for over 25k decisions, we can also leverage Snowflake's Cortex Large Language Model (LLM) functionality.  This gives us a good view into how we can leverage AI for enriching our supply chain data in an effort to understand the broader context of the supply chain.  Our examples add a few new fields that are generated through prompts.

**Note** - AI is an imprecise science that is improving every day, so it is important to check results for reasonableness and to understand that values are approximate.  It is perfect for adding some additional flavor to our data, but is not ideal for analyzing our model results.

## Directions
1. Run app_setup.sql as ACCOUNTADMIN in your Snowflake account
2. In Snowsight, set your role to SCNO_ROLE, navigate to Projects on the left hand bar, and select Streamlit
3. Select SUPPLY_CHAIN_NETWORK_OPTIMIZATION in your list of Streamlit Apps
4. Follow in-app instructions
