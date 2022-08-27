from kafka.admin import KafkaAdminClient, NewTopic


def create_topic(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092',
    client_id='tests_id',


    )

    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=2))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    return admin_client.list_topics()

if __name__ == "__main__" :
    
    top=input("Input_topic_Name")
    create_topic(top)