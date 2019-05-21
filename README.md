Spark Structured Streaming

Using as input data the data from Málaga Parking, https://datosabiertos.malaga.eu/recursos/aparcamientos/ocupappublicosmun/ocupappublicosmun.csv develop a PySpark application that selects the data of each parking only when the parking has a capacity higher than zero. The result must be only updated when new data are showing up.

The outcoming data frames have to contain the following fields: name, capacity and number of available slots (free slots).
