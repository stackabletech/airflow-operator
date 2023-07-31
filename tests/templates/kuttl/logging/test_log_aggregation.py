#!/usr/bin/env python3
import requests


def check_sent_events():
    response = requests.post(
        'http://airflow-vector-aggregator:8686/graphql',
        json={
            'query': """
                {
                    transforms(first:100) {
                        nodes {
                            componentId
                            metrics {
                                sentEventsTotal {
                                    sentEventsTotal
                                }
                            }
                        }
                    }
                }
            """
        }
    )

    assert response.status_code == 200, \
        'Cannot access the API of the vector aggregator.'

    result = response.json()

    transforms = result['data']['transforms']['nodes']
    for transform in transforms:
        sentEvents = transform['metrics']['sentEventsTotal']['sentEventsTotal']
        componentId = transform['componentId']
        assert sentEvents > 0, \
            f'No events were sent in "{componentId}".'


if __name__ == '__main__':
    check_sent_events()
    print('Test successful!')
