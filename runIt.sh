docker stop sermonpreprocessor
docker rm sermonpreprocessor

docker build -t sermonpreprocessor .

docker run -dit --restart unless-stopped --name sermonpreprocessor -p 5060:5060 -v ./data:/data sermonpreprocessor:latest 
