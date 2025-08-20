# Message system

> [!NOTE]
> This application assume that you have rabbitmq installed in your Kubernetes cluster and you have access to a S3 service. If you don't have rabbitmq installed in your Kubernetes cluster, please check https://www.rabbitmq.com/kubernetes/operator/using-operator for installation.

## Deployment

1. First copy the value file for helm template

    ```console
    cp helm/message-sync/values.templ.yaml helm/message-sync/values.yaml
    ```

1. Set-up all your values helm/message-sync/values.templ.yaml in "set-me"

1. Build and push docker images

    ```console
    export REGISTRY=<your image registry>

    docker build -t ${REGISTRY}/queue/writer:v1 apps/writer
    docker build -t ${REGISTRY}/queue/reader:v1 apps/reader

    docker push ${REGISTRY}/queue/writer:v1
    docker push ${REGISTRY}/queue/reader:v1
    ```

1. Deploy your application

    ```console
    kubectl create ns message-app
    helm -n message-app upgrade --install message-app helm/message-sync
    ```