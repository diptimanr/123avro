cc_config = {
    'bootstrap.servers': '<YOUR_BOOTSTRAP_SERVER>:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '<CLUSTER_API_KEY>',
    'sasl.password': '<CLUSTER_API_SECRET>'
}

sr_config = {
    'url': '<SCHEMA_REGISTRY_ENDPOINT>',
    'basic.auth.user.info': '<SR_API_KEY>:<SR_API_SECRET>'
}