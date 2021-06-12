# gifmaker-create-image-tags

### Manual deployment

docker build -t gif-maker-create-image-tags:latest .

docker login -u AWS -p $(aws ecr get-login-password --region us-east-1) 201374217398.dkr.ecr.us-east-1.amazonaws.com
docker tag gif-maker-create-image-tags:latest 201374217398.dkr.ecr.us-east-1.amazonaws.com/gif-maker-create-image-tags:latest
docker push 201374217398.dkr.ecr.us-east-1.amazonaws.com/gif-maker-create-image-tags:latest

aws lambda update-function-code --region us-east-1 --function-name create-image-tags \
    --image-uri 201374217398.dkr.ecr.us-east-1.amazonaws.com/gif-maker-create-image-tags:latest