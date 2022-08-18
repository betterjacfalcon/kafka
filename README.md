# kafka
kafka: better pratices...
~~~
El grupo-id nos permite determinar cual mensaje fueron leidos
~~~
~~~
Es importante saber que en caso que se requiera dar una confirmacion si el mensaje fue entregado el proceso puede seguir siendo asincrono pero se debe programar un callback para indicar que ya fue entregado el mensaje
Una opcion es programar una clase anonima
~~~