### Como executar o projeto.

Para ativar o venv dentro da pasta do projeto executar:

Para Windows
~~~sh
venv/Scripts/Activate
~~~

Para Linux ou macOS
~~~sh
source nome_da_virtualenv/bin/activate
~~~

https://sci-hub.hkvisa.net/10.1186/s13638-019-1511-4

- Estruturar melhor o conceito de outlier global e local.

O outlier global é encontrado quando é detectado desvio em todas as médidas da leitura do sensor
O outlier local é encontrado quando pelo menos uma das medicas encontram desvios da medida do sensor

calculado o desvio padrão do grafico de boxplot 1,5 - 1Q e 1,5 + Q3

wavelet threshold denoising method Transforma dados de leituras de sensores em dados "mais analógicos"

https://www.mdpi.com/2075-5309/11/6/263

https://github.com/bartk97/NYC-Taxi-Anomaly-Detection (Muito bom)

ST_ClusterKMeans
ST_DWithin
ST_ClusterDBSCAN

aplicar o dbscan nos dados dos sensores para agrupar cluster 

fazer um modelo para cada configuração especifica de sensor

criar novas métricas de distnacia, dencidade e georeferenciamento, desvio padrão sei lá...

Aplicar o auto escaler, talvez o T-NSE

Colocar dentro do isolon forent e LSTM e fim