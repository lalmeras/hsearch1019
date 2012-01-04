package hsearch1019.test;

import hsearch1019.entity.Node;

import org.apache.lucene.util.Version;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.HSQLDialect;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.MassIndexer;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.impl.FullTextSessionImpl;
import org.hibernate.search.store.impl.FSDirectoryProvider;
import org.hibernate.service.ServiceRegistryBuilder;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHSearch1019 {

	private static final Logger LOGGER = LoggerFactory.getLogger(TestHSearch1019.class);

	private static final int ENTITY_COUNT = 5000;

	@Test
	public void testHSearch1019() throws InterruptedException {
		Configuration configuration = new Configuration();
//		configuration.setProperty("hibernate.dialect", PostgreSQLDialect.class.getName());
//		configuration.setProperty("hibernate.connection.driver_class", Driver.class.getName());
//		configuration.setProperty("hibernate.connection.url", "jdbc:postgresql://localhost:5432/hsearch1019");
		configuration.setProperty("hibernate.dialect", HSQLDialect.class.getName());
		configuration.setProperty("hibernate.connection.driver_class", JDBCDriver.class.getName());
		configuration.setProperty("hibernate.connection.url", "jdbc:hsqldb:mem:hsearch1019");
		
		configuration.setProperty("hibernate.hbm2ddl.auto", "create");
		configuration.setProperty("hibernate.connection.username", "hsearch1019");
		configuration.setProperty("hibernate.connection.password", "hsearch1019");
		configuration.setProperty("hibernate.connection.pool_size", "10");
		
		configuration.setProperty("hibernate.search.default.directory_provider", FSDirectoryProvider.class.getName());
		configuration.setProperty("hibernate.search.default.indexBase", "/tmp/hsearch1019");
		String luceneVersion = Version.values()[Version.values().length - 2].name();
		LOGGER.info("Use lucene_version {}", luceneVersion);
		configuration.setProperty("hibernate.search.lucene_version", luceneVersion);
		
		configuration.addAnnotatedClass(Node.class);
		
		LOGGER.info("Creating session.");
		SessionFactory sessionFactory = configuration.buildSessionFactory(
				new ServiceRegistryBuilder().applySettings(configuration.getProperties()).buildServiceRegistry()
		);
		
		Session session;
		LOGGER.info("Start creation.");
		{
			session= sessionFactory.openSession();
			Transaction t = session.beginTransaction();
			for (int i = 0; i < ENTITY_COUNT; i++) {
				session.save(new Node());
			}
			session.flush();
			t.commit();
			session.close();
		}
		LOGGER.info("Creation done.");
		
		LOGGER.info("Start entity check.");
		{
			long startTime = System.currentTimeMillis();
			session = sessionFactory.openSession();
			Assert.assertTrue(session.createCriteria(Node.class).list().size() == ENTITY_COUNT);
			session.close();
			long endTime = System.currentTimeMillis();
			LOGGER.info("Entities checked in {} ms.", endTime - startTime);
		}
		
		
		{
			session = sessionFactory.openSession();
			FullTextSession fullTextSession = new FullTextSessionImpl(session);
			MassIndexer indexer = fullTextSession.createIndexer();
			ProgressMonitor progressMonitor = new ProgressMonitor();
			Thread t = new Thread(progressMonitor);
			t.start();
			indexer.batchSizeToLoadObjects(1)
					.threadsForSubsequentFetching(1)
					.threadsToLoadObjects(1)
					.cacheMode(CacheMode.NORMAL)
					.progressMonitor(progressMonitor);
			LOGGER.info("Start mass index job.");
			long startTime = System.currentTimeMillis();
			indexer.startAndWait();
			t.interrupt();
			progressMonitor.stop();
			long endTime = System.currentTimeMillis();
			LOGGER.info("Indexation last for {} ms.", endTime - startTime);
			session.close();
		}
		
		sessionFactory.close();
	}
	
	private static final class ProgressMonitor implements MassIndexerProgressMonitor, Runnable {
		
		private static final Logger LOGGER = LoggerFactory.getLogger(ProgressMonitor.class);
		
		private long documentsAdded;
		private int documentsBuilt;
		private int entitiesLoaded;
		private int totalCount;
		private boolean indexingCompleted;
		private boolean stopped;
		
		@Override
		public void documentsAdded(long increment) {
			this.documentsAdded += increment;
		}
		
		@Override
		public void documentsBuilt(int number) {
			this.documentsBuilt += number;
		}
		
		@Override
		public void entitiesLoaded(int size) {
			this.entitiesLoaded = size;
		}
		
		@Override
		public void addToTotalCount(long count) {
			this.totalCount += count;
		}
		
		@Override
		public void indexingCompleted() {
			this.indexingCompleted = true;
		}
		
		public void stop() {
			this.stopped = true;
			log();
		}
		
		@Override
		public void run() {
			try {
				while (true) {
					log();
					Thread.sleep(15000);
					if (indexingCompleted) {
						LOGGER.info("Indexing done");
						break;
					}
				}
			} catch (Exception e) {
				if (!stopped) {
					LOGGER.error("Error ; massindexer monitor stopped", e);
				}
				LOGGER.info("Massindexer monitor thread interrupted");
			}
		}
		
		private void log() {
			LOGGER.info(String.format("Indexing %1$d / %2$d (entities loaded: %3$d, documents built: %4$d)",
					documentsAdded, totalCount, entitiesLoaded, documentsBuilt));
		}
	}
}
