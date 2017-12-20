import boto3
import timeit

def main():
    # Create SQS client
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/272471147916/canvas-live-events-for-umich-analytics'
    # queue_url = 'https://sqs.us-east-1.amazonaws.com/272471147916/pushyami-test-queue'
    # queue_url = 'https://sqs.us-east-1.amazonaws.com/154191975022/ProdAnalyticsRelay-MichiganProd-out'

    # Receive message from SQS queue

    get = 0
    while True:
        start = timeit.default_timer()
        response = get_sqs_msgs(queue_url, sqs)
        stop = timeit.default_timer()
        response_code = response['ResponseMetadata']['HTTPStatusCode']

        if response_code != 200:
            print('response code: '+response_code)
            continue
        # if we have message then process the message and delete them
        if 'Messages' not in response:
            print('Total Events: '+str(get))
            print("All Messages extracted from Queue")
            exit(0)

        messages = response['Messages']

        if messages:
            # for msg in messages:
            for i in range(len(messages)):
                save_n_delete_msgs(i, messages, queue_url, sqs)
            print('items in the each run: '+str(i + 1))
            print('Response Time: '+str(stop-start))
            get=(get+1)+i
            print('Total Events until now: '+str(get))


def get_sqs_msgs(queue_url, sqs):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        AttributeNames=[
            'All'
        ],
        VisibilityTimeout=10,
        WaitTimeSeconds=10
    )
    return response


def save_n_delete_msgs(i, messages, queue_url, sqs):
    messages_i_ = messages[i]
    receipt_handle = messages_i_['ReceiptHandle']
    with open('live-events-body-only.jsonl', 'a') as the_file:
        the_file.write(messages_i_['Body'])
        the_file.write('\n')
    with open('live-events-as-is-from-sqs-q.txt', 'a') as the_file1:
        the_file1.write(''.join('{}:{} '.format(key, val) for key, val in messages_i_.items()))
        the_file1.write('\n')
        # Delete received message from queue
    delete_msg(queue_url, receipt_handle, sqs)


def delete_msg(queue_url, receipt_handle, sqs):
    deleted_response = sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    with open('live-events-delete-info.jsonl', 'a') as the_file:
        the_file.write(deleted_response['ResponseMetadata']['RequestId'])
        the_file.write('\n')


if __name__ == '__main__':
    main()
