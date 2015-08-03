JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	edu/cmu/ds/p3/configuration/Config.java \
	edu/cmu/ds/p3/example/NumCountByOddOrEvenMapper.java \
	edu/cmu/ds/p3/example/NumCountByOddOrEvenReducer.java \
	edu/cmu/ds/p3/example/WordCountMapper.java \
	edu/cmu/ds/p3/example/WordCountReducer.java \
	edu/cmu/ds/p3/Interface/Mapper.java \
	edu/cmu/ds/p3/Interface/Reducer.java \
	edu/cmu/ds/p3/util/MapTask.java \
	edu/cmu/ds/p3/util/ReduceTask.java \
	edu/cmu/ds/p3/util/Task.java \
	edu/cmu/ds/p3/util/TmpKV2TmpResult.java \
	edu/cmu/ds/p3/util/InputSplit.java \
	edu/cmu/ds/p3/util/TmpResult.java \
	edu/cmu/ds/p3/util/TmpKVPair.java \
	edu/cmu/ds/p3/util/Message.java \
	edu/cmu/ds/p3/util/Record.java \
	edu/cmu/ds/p3/util/RecordReader.java \
	edu/cmu/ds/p3/util/RecordWriter.java \
	edu/cmu/ds/p3/master/Master.java \
	edu/cmu/ds/p3/master/MasterCommandHandler.java \
	edu/cmu/ds/p3/master/MasterServer.java \
	edu/cmu/ds/p3/master/MasterManager.java \
	edu/cmu/ds/p3/master/HealthyChecker.java \
	edu/cmu/ds/p3/slave/Slave.java \
	edu/cmu/ds/p3/slave/SlaveCommandHandler.java \
	edu/cmu/ds/p3/slave/SlaveServer.java \
	edu/cmu/ds/p3/slave/SlaveManager.java \
    edu/cmu/ds/p3/slave/SlaveInputFileTransListener.java \
	edu/cmu/ds/p3/slave/SlaveInterResTransListener.java \
	edu/cmu/ds/p3/combiner/Sorter.java \
	edu/cmu/ds/p3/combiner/Shuffler.java \

default: classes
		 jar cf mapreduce.jar *

classes: $(CLASSES:.java=.class)

master:
	java -cp mapreduce.jar edu.cmu.ds.p3.master.MasterManager edu/cmu/ds/p3/configuration/config.properties

slave_1:
	java -cp mapreduce.jar edu.cmu.ds.p3.slave.SlaveManager slave_1 edu/cmu/ds/p3/configuration/config.properties

slave_2:
	java -cp mapreduce.jar edu.cmu.ds.p3.slave.SlaveManager slave_2 edu/cmu/ds/p3/configuration/config.properties

slave_3:
	java -cp mapreduce.jar edu.cmu.ds.p3.slave.SlaveManager slave_3 edu/cmu/ds/p3/configuration/config.properties

clean:
	rm -rf edu/cmu/ds/p3/*/*.class
	rm mapreduce.jar
	rm -r tmp
	
	
