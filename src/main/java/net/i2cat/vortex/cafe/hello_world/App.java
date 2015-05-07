package net.i2cat.vortex.cafe.hello_world;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.BasicConfigurator;
import org.omg.dds.core.DDSException;
import org.omg.dds.core.Duration;
import org.omg.dds.core.ServiceEnvironment;
import org.omg.dds.core.policy.Durability;
import org.omg.dds.core.policy.DurabilityService;
import org.omg.dds.core.policy.History;
import org.omg.dds.core.policy.LatencyBudget;
import org.omg.dds.core.policy.PolicyFactory;
import org.omg.dds.core.status.Status;
import org.omg.dds.domain.DomainParticipant;
import org.omg.dds.domain.DomainParticipantFactory;
import org.omg.dds.pub.DataWriter;
import org.omg.dds.pub.Publisher;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.Sample.Iterator;
import org.omg.dds.sub.Subscriber;
import org.omg.dds.topic.Topic;
import org.omg.dds.topic.TopicQos;
import org.omg.dds.type.TypeSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class
 * 
 * @author Julio Carlos Barrera (i2CAT Foundation)
 *
 */
public class App {

	static private final Logger log = LoggerFactory.getLogger(App.class);

	static private final int DS_HISTORY = 100;
	static private final int DS_MAX_SAMPLES = 8192;
	static private final int DS_MAX_INSTANCES = 4196;
	static private final int DS_MAX_SAMPLES_X_INSTANCE = 8192;
	static private final long CLEAN_UP_DELAY = 3600;

	private int domainId;
	private String partition;

	private Topic<Data> dataTopic;

	private DomainParticipantFactory dpf = null;
	private DomainParticipant dp = null;
	private PolicyFactory pf = null;
	private Publisher pub = null;
	private Subscriber sub = null;

	public App() {

		// get domainId and partition
		domainId = Integer.parseInt(System.getProperty("dds.domainId", "0"));
		partition = System.getProperty("dds.partition", "");

		log.info("DDS will use Domain {} and Partition \"{}\"", domainId,
				partition);
	}

	public void init() throws DDSException {
		// Select DDS implementation and initialize DDS ServiceEnvironment
		log.info("Load DDS implementation");
		System.setProperty("ddsi.readers.heartbeatResponseDelay", "0.0");
		System.setProperty("ddsi.writers.nackResponseDelay", "0.0");

		System.setProperty(
				ServiceEnvironment.IMPLEMENTATION_CLASS_NAME_PROPERTY,
				"com.prismtech.cafe.core.ServiceEnvironmentImpl");
		ServiceEnvironment env = ServiceEnvironment.createInstance(App.class
				.getClassLoader());

		// create Participant
		log.info("Create Participant");
		dpf = DomainParticipantFactory.getInstance(env);
		dp = dpf.createParticipant(domainId);

		// get PolicyFactory
		pf = PolicyFactory.getPolicyFactory(env);

		// create TypeSupport to use a specific name for type registration
		TypeSupport<Data> typeSupport = TypeSupport.newTypeSupport(Data.class,
				"Data", env);

		// create topics with Persistent durability
		log.info("Create Topics");
		Durability durability = pf.Durability().withPersistent();
		DurabilityService ds = pf.DurabilityService()
				.withHistoryDepth(DS_HISTORY)
				.withServiceCleanupDelay(CLEAN_UP_DELAY, TimeUnit.SECONDS)
				.withHistoryKind(History.Kind.KEEP_LAST)
				.withMaxInstances(DS_MAX_INSTANCES)
				.withMaxSamples(DS_MAX_SAMPLES)
				.withMaxSamplesPerInstance(DS_MAX_SAMPLES_X_INSTANCE);

		TopicQos tq = dp.getDefaultTopicQos().withPolicy(durability)
				.withPolicy(ds);
		dataTopic = dp.createTopic("DataTopic", typeSupport, tq, null,
				(Collection<Class<? extends Status>>) null);

		// create publisher
		log.info("Create Publisher");
		pub = dp.createPublisher(dp.getDefaultPublisherQos().withPolicy(
				pf.Partition().withName(Arrays.asList(partition))));

		// create subscriber
		log.info("Create Subscriber");
		sub = dp.createSubscriber(dp.getDefaultSubscriberQos().withPolicy(
				pf.Partition().withName(Arrays.asList(partition))));
	}

	public DataReader<Data> getDataReader() {
		log.info("Create DataReader");
		LatencyBudget latency = pf.LatencyBudget().withDuration(
				Duration.infiniteDuration(dp.getEnvironment()));
		return sub.createDataReader(dataTopic, sub.getDefaultDataReaderQos()
				.withPolicy(latency));
	}

	public DataWriter<Data> getDataWriter() {
		log.info("Create DataWriter");
		return pub.createDataWriter(dataTopic, pub.getDefaultDataWriterQos());
	}

	class PublisherThread implements Runnable {
		private App app;

		public PublisherThread(App app) {
			this.app = app;
		}

		public void run() {
			log.info("Starting publisher thread...");

			DataWriter<Data> dw = app.getDataWriter();

			try {
				int seq = 0;
				while (true) {
					try {
						int seqId = seq++;
						int randomA = (int) (Math.random() * ((1000) + 1));
						String randomB = UUID.randomUUID().toString();

						log.info("Writing data: id = {}, a = {}, b = \"{}\".",
								seqId, randomA, randomB);
						dw.write(new Data(seqId, randomA, randomB));
						Thread.sleep(100);
					} catch (TimeoutException e) {
						log.warn("Timeout exception", e);
						break;
					}
				}
			} catch (InterruptedException e) {
				log.warn("Interrupted!");
			}
		}
	}

	class SubscriberThread implements Runnable {
		private App app;

		public SubscriberThread(App app) {
			this.app = app;
		}

		public void run() {
			log.info("Starting subscriber thread...");
			DataReader<Data> dr = app.getDataReader();

			try {
				while (true) {
					Iterator<Data> dIt = dr.read();
					while (dIt.hasNext()) {
						Data data = dIt.next().getData();
						log.info("Data read: id = {}, a = {}, b = \"{}\".",
								data.id, data.a, data.b);
						Thread.sleep(100);
					}
				}
			} catch (InterruptedException e) {
				log.warn("Interrupted!");
			}
		}
	}

	public static final void printUsageAndExit() {
		log.error("Parameters expected are: \"sX pY\", where X is the number of subscriber threads and Y the number of publisher threads.");
		System.exit(1);
	}

	public static void main(String[] args) throws InterruptedException {
		// Set up a simple configuration that logs on the console.
		BasicConfigurator.configure();

		// parse subscribers and publishers
		int subs = 0, pubs = 0;

		try {
			for (String arg : args) {
				if (arg.startsWith("s")) {
					subs = Integer.parseInt(arg.substring(1));
				} else if (arg.startsWith("p")) {
					pubs = Integer.parseInt(arg.substring(1));
				}
			}
		} catch (Exception e) {
			log.error("Error parsing paramaters", e);
			printUsageAndExit();
		}

		if (subs <= 0 && pubs <= 0) {
			log.error("Use at least one publisher or one subscriber");
			printUsageAndExit();
		}

		log.info("Goig to start " + subs + " subscribers and " + pubs
				+ " publishers.");

		List<Thread> pubsThreadList = new ArrayList<Thread>();
		List<Thread> subsThreadList = new ArrayList<Thread>();

		log.info("START");

		App app = new App();
		app.init();

		for (int s = 0; s < subs; s++) {
			Thread st = new Thread(app.new SubscriberThread(app), "S" + s);
			subsThreadList.add(st);
			st.start();
		}

		for (int p = 0; p < pubs; p++) {
			Thread pt = new Thread(app.new PublisherThread(app), "P" + p);
			pubsThreadList.add(pt);
			pt.start();
		}

		for (Thread st : subsThreadList) {
			st.join();
		}

		for (Thread pt : pubsThreadList) {
			pt.join();
		}

		log.info("END");
	}
}
