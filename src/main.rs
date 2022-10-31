use clap::Parser;
use futures_lite::stream::StreamExt;
use lapin::{Result, Connection, ConnectionProperties, options::{BasicConsumeOptions, BasicAckOptions, QueueDeclareOptions}, types::FieldTable};
use model::User;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value="amqp://127.0.0.1:5672/%2f")]
    ampq_addr: String,
    #[arg(short, long, default_value="myqueue")]
    queue_name: String
}


fn main() -> Result<()> {
    let args = Args::parse();

    async_global_executor::block_on(async {
        let conn = Connection::connect(
            &args.ampq_addr,
            ConnectionProperties::default(),
        ).await?;

        let channel = conn.create_channel().await?;
        
        let _ = channel
            .queue_declare(
                &args.queue_name, QueueDeclareOptions::default(), FieldTable::default(),
            ).await?;

        let mut consumer = channel.basic_consume(
            &args.queue_name,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ).await?;

        async_global_executor::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                let delivery = delivery.expect("error in consumer");
                let payload = User::deserialize(&delivery.data);
                println!("Received {:?}", payload);
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("sending ack failed");
            }
        }).await;

        Ok(())
    })
}
