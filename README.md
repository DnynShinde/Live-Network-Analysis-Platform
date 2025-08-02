# real_time_dtn

## Create .env file and add LLM API key

Create a file named `.env` and add the following:

```env
GOOGLE_API_KEY=your-llm-api-key
```
## Running Instruction
``` bash
# Run the Docker Compose
>docker-compose.yml up

# Run the kafka consumer
>python3 -m kafka_app.consumer

#Run the SDN poller to build KG
>python3 - m sdn_poller.api_poller

# To query the neo4j to get the info
>python3 -m user_app.app
