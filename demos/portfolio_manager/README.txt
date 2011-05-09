Brisk Portfolio Manager Demo
============================

The portfolio manager demonstrates a hybrid workflow: live changing data(OLTP)
and backend analytics(OLAP).

Our use case is a finance application where customers are actively creating and
managing their portfolios of stocks.

The OLTP Side:
  Each portfolio contains a list of stocks, the number of shares purchased, 
and the price at which they were purchased.

  In our system there is a continuous stream of live price data flowing in
simulating an active market.  Each portfolio can be priced to get the 
overall value based on the current market prices and the percent gain 
or loss since the purchase price.

  At the same time we maintain historical market data every stock, this being
the end-of-day price for each stock going back in time.

  With this historical data we can also the historical performance of each
portfolio.

For this demo the data is all simulated by inserting data into our cassandra
side using the ./bin/pricer tool which creates portfolios, live prices and
historical prices.

The OLAP Side:
  When people value a portfolio they don't just look at price information, 
they want to understand how "risky" their portfolio is.  In this demo we 
ask the question: 

   "What is the most money each portfolio has lost historically if it was held
   it for 10 days?"

Using Brisk and Hive we can easily calculate this value for each portfolio and
insert the result back into cassandra for users to see.  Now users have a much
better understanding of the potential losses they would incur by holding onto
their portfolio.

Running The Demo
================

Starting the web-service
------------------------

First build and start brisk:

 ant
 ./bin/brisk cassandra -t

Next, build the demo

 cd demos/portfolio_manager
 ant

Next, add some data to the column families

 ./bin/pricer -o INSERT_PRICES
 ./bin/pricer -o UPDATE_PORTFOLIOS
 ./bin/pricer -o INSERT_HISTORICAL_PRICES -n 100

NOTE: if you are running on a cluster the first pricer call should be the
following:

./bin/pricer -o INSERT_PRICES
             --replication-strategy="org.apache.cassandra.locator.NetworkTopologyStrategy" 
             --strategy-properties="Brisk:1,Cassandra:1"

Now, start the web-service:

 cd website
 java -jar start.jar

At this You can point your webbrowser to http://localhost:8983/portfolio You
should see some portfolios and some charts reflecting the portfolio holdings
and the price chart.

If you want you can update the portfolios or insert new prices using the above
scripts the site will update in realtime.

If you want to update the prices continuously you can run the following:

while true; do ./bin/pricer; sleep 1; done

Running Hive
------------

Now, to run a hive calculation do the following in another shell

 ./bin/brisk hive -f demos/portfolio_manager/10_day_loss.q

This will run for 5-10 minutes and calculate the worst 10 day period over the
past 100 days for each portfolio. Once finished, when you reload the website,
each portfolio should show its worst period and loss.
