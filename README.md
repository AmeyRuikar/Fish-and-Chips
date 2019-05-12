# Fish-&-Chips: StreamOrdering 

In this example, I'm `stream processing` food orders. An order of Fish-and-chips has two components, which might be processed by two different kitchens. However, we should notify(or bill) the customer once both are ready. 

Once each of the kitchen is ready with their parts of the order, they will emit a `message` in our system. Our goal here is to process the order only when ***both*** the parts are ready for a given `order_id`. 

The `message` will be in the following format:
> (timestamp, order_id, item, portions) -> (1557640618000, 4311, fish, 2)

And this data would be flowing over a `Kafka` topic. 

Once we have the corresponding parts of order we will generate a bill/check for the customer, assuming `$9.80` for a portion.
