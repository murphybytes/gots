# gots

Gots is an in memory cache optimized for storage of time series data.  Many data stores are designed to handle many
reads and few writes which causes difficulty handling things like streaming real time stock quotes.  Gots is optimized
to handle many writes and few reads and so is ideal for this type of data.



## Development

**gots** uses [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) which requires a shared library 
**librdkafka** to run.  See the [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) for installation
instructions. 

Generate files and update dependencies 

`make init`

Run Tests 

`make test`

`make test-race`

