Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

---------

(Preliminar, editar despues)

## Problemas encontrados:

### Ejercicio 3:
Aca se nos pedia que los inputs de los clientes se lean desde archivos de input y las respuestas del servidor se escriban a archivos de output, al principio queria encontrar los archivos simplemente escribiendo el path "absoluto" pero despues mirando los dockerfile me di cuenta que no se podia porque las carpetas de input y output no se incluian en el contenedor cuando se creaba, entonces agregue "volumes" en el docker-compose.yaml, tanto de los inputs como de los outputs.

Como convencion (y mas que nada al ver que habia archivos de input con la misma nomenclatura que los clientes), cada cliente usa el archivo de input "input-X.csv", donde X es su AgencyId que lo lee como variable de entorno del docker-compose.yaml.

### Ejercicio 4:
Cuando quise mejorar las funciones de Recv y Send tanto del cliente como del server, mi primera solucion fue hacer que solo se lea hasta size en el caso del Recv y solo se escriba hasta len(bytes) en el caso del send. El problema de esto es que hay una constante que define (del lado del server) cuanto se va a leer en recv_all, entonces cuando el cliente envia cierta cantidad de bytes variable, la funcion se queda colgada esperando el resto (a menos que justo coincidan las cantidades).

Leyendo el enunciado vi que en el ejercicio 5 se nos pide quitar esas constantes, entonces aproveche que igual iba a tener que acomodar el codigo para despues e hice emisores y receptores "dinamicos".

Para este modelo dinamico me voy a basar en el protocolo RTP que utiliza un header que describe el tamaño del resto del paquete (en este caso mensaje).