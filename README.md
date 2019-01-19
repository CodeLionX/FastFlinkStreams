# FastFlinkStreams
A Flink demo project using Scala and SBT that analyzes HTTP log data from [NASA HTTP](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html).

## Usage

To run and test the application locally, you can just execute `sbt run`.
You can also package the application into a fat jar with `sbt assembly`, then submit it to a flink cluster. 

You can also run the application from within IntelliJ by selecting the classpath of the 'mainRunner' module in the run/debug configurations
- open "Run -> Edit configurations..."
- click on "+" to create a new configuration for an "Application"
- type in name and select the main class to run
- select 'mainRunner' from the "Use classpath of module" dropbox
