This code is based on the implementation you can find in the following repository:

- https://github.com/sekruse/profiledb-java.git

The code there does not have regular maintenance. Wayang will require new functionalities to deal with serialization of UDFs and storage in other platforms.

The classes below has not been modified:
 
    MeasurementDeserializer
    MeasurementSerializer
    Experiment
    Measurement
    Subject
    TimeMeasurement
    Type
    
The classes below has been added/modified to provide an abstraction over different Storage methods:
  
    ProfileDB
    Storage
    FileStorage
    JDBCStorage
    
The code that is based on the mentioned repository starts and ends with the commits indicated below:

- start: [5344336f68bb9038e701435e9859321b6e8cbcfc](https://github.com/apache/incubator-wayang/commit/5344336f68bb9038e701435e9859321b6e8cbcfc)

- end: [fdb7ac352c5fb7eb69a436a5b4dc42c8d9d0a268](https://github.com/apache/incubator-wayang/commit/fdb7ac352c5fb7eb69a436a5b4dc42c8d9d0a268)

All the code that has been added after those commits is totally independent of the mentioned repository. 