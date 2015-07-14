/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.arquillian.persistence.dbunit;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.datatype.DefaultDataTypeFactory;
import org.jboss.arquillian.core.api.Instance;
import org.jboss.arquillian.core.api.InstanceProducer;
import org.jboss.arquillian.core.api.annotation.Inject;
import org.jboss.arquillian.core.api.annotation.Observes;
import org.jboss.arquillian.core.spi.EventContext;
import org.jboss.arquillian.persistence.DataSourceWithData;
import org.jboss.arquillian.persistence.DataSourcesWithData;
import org.jboss.arquillian.persistence.core.event.AfterPersistenceTest;
import org.jboss.arquillian.persistence.core.event.BeforePersistenceTest;
import org.jboss.arquillian.persistence.core.lifecycle.PersistenceTestTrigger.DataSourceMap;
import org.jboss.arquillian.persistence.core.metadata.MetadataExtractor;
import org.jboss.arquillian.persistence.core.metadata.PersistenceExtensionFeatureResolver;
import org.jboss.arquillian.persistence.dbunit.configuration.DBUnitConfiguration;
import org.jboss.arquillian.persistence.dbunit.configuration.DBUnitConfigurationPropertyMapper;
import org.jboss.arquillian.persistence.dbunit.data.descriptor.DataSetResourceDescriptor;
import org.jboss.arquillian.persistence.dbunit.data.provider.DataSetProvider;
import org.jboss.arquillian.persistence.dbunit.data.provider.ExpectedDataSetProvider;
import org.jboss.arquillian.persistence.dbunit.dataset.DataSetRegister;
import org.jboss.arquillian.persistence.dbunit.exception.DBUnitInitializationException;
import org.jboss.arquillian.test.spi.annotation.ClassScoped;
import org.jboss.arquillian.test.spi.annotation.TestScoped;

/**
 * @author <a href="mailto:bartosz.majsak@gmail.com">Bartosz Majsak</a>
 */
public class DBUnitPersistenceTestLifecycleHandler {
	/* TODO move to own class */
	public static class DatabaseConnectionList {
		private List<DatabaseConnection> connections;

		public List<DatabaseConnection> getConnections() {
			return this.connections;
		}

		public void setConnections(final List<DatabaseConnection> connections) {
			this.connections = connections;
		}

		public DatabaseConnectionList() {
			this.connections = new LinkedList<DatabaseConnection>();
		}

		public DatabaseConnectionList(final List<DatabaseConnection> connections) {
			this.connections = connections;
		}

	}

	public static class DataSetRegisterList {
		private List<DataSetRegister> dataSetRegisters;

		public DataSetRegisterList() {
			this.dataSetRegisters = new LinkedList<DataSetRegister>();
		}

		public DataSetRegisterList(final List<DataSetRegister> dataSetRegisters) {
			this.dataSetRegisters = dataSetRegisters;
		}

		public List<DataSetRegister> getDataSetRegisters() {
			return this.dataSetRegisters;
		}

		public void setDataSetRegisters(final List<DataSetRegister> dataSetRegisters) {
			this.dataSetRegisters = dataSetRegisters;
		}

	}

   @Inject
	private Instance<DataSourceMap> dataSourceInstance;

   @Inject
   private Instance<MetadataExtractor> metadataExtractorInstance;

   @Inject
   private Instance<DBUnitConfiguration> dbUnitConfigurationInstance;

   @Inject
   @ClassScoped
	private InstanceProducer<DatabaseConnectionList> databaseConnectionProducer;

   @Inject
   @TestScoped
	private InstanceProducer<DataSetRegisterList> dataSetRegisterProducer;

   @Inject
   private Instance<PersistenceExtensionFeatureResolver> persistenceExtensionFeatureResolverInstance;

	private Map<DataSource, DatabaseConnection> dataSoruceTodatabaseConnection;

	// ------------------------------------------------------------------------------------------------
	// Intercepting data handling events
	// ------------------------------------------------------------------------------------------------

   public void provideDatabaseConnectionAroundBeforePersistenceTest(@Observes(precedence = 100000) EventContext<BeforePersistenceTest> context)
   {
      createDatabaseConnection();
      context.proceed();
   }

   public void closeDatabaseConnections(@Observes(precedence = 100000) EventContext<AfterPersistenceTest> context)
   {
      try
      {
         context.proceed();
      } finally
      {
         closeDatabaseConnection();
      }
   }

	public void createDatasets(@Observes(precedence = 1000) final EventContext<BeforePersistenceTest> context) {
		final Method testMethod = context.getEvent().getTestMethod();

		final PersistenceExtensionFeatureResolver persistenceExtensionFeatureResolver = this.persistenceExtensionFeatureResolverInstance
				.get();
		if (persistenceExtensionFeatureResolver.shouldSeedData()) {
			if (persistenceExtensionFeatureResolver.hasMultipleDataSources()) {
				final DataSourcesWithData dataSourcesWithData = this.metadataExtractorInstance.get()
						.dataSourcesWithData().fetchFrom(testMethod);
				final DataSetProvider dataSetProvider = new DataSetProvider(this.metadataExtractorInstance.get(),
						this.dbUnitConfigurationInstance.get());
				final Collection<DataSetResourceDescriptor> dataSetDescriptors = dataSetProvider
						.getDescriptorsDefinedFor(testMethod);
				for (final DataSourceWithData dataSourceWithData : dataSourcesWithData.value()) {
					final String dataSourceName = dataSourceWithData.source().value();
					final DataSource dataSource = this.dataSourceInstance.get().findByName(dataSourceName);
					final DatabaseConnection databaseConnection = this.dataSoruceTodatabaseConnection.get(dataSource);
					final LinkedList<DataSetResourceDescriptor> dataSourceDataSetResourceDescriptors = new LinkedList<DataSetResourceDescriptor>();
					for (final String dataSetResourceDescriptorName : dataSourceWithData.usingDataSet().value()) {
						for (final DataSetResourceDescriptor dataSetResourceDescriptor : dataSetDescriptors) {
							// Ends with
							if (dataSetResourceDescriptor.getLocation().endsWith(dataSetResourceDescriptorName)) {
								dataSourceDataSetResourceDescriptors.add(dataSetResourceDescriptor);
							}
						}
					}

					this.createInitialDataSets(databaseConnection, dataSourceDataSetResourceDescriptors);
					/* Map the file name to the datasource */
				}

			} else {
				for (final DatabaseConnection databaseConnection : this.databaseConnectionProducer.get()
						.getConnections()) {
					final DataSetProvider dataSetProvider = new DataSetProvider(this.metadataExtractorInstance.get(),
							this.dbUnitConfigurationInstance.get());
					this.createInitialDataSets(databaseConnection, dataSetProvider.getDescriptorsDefinedFor(testMethod));
				}
			}
		}

		if (persistenceExtensionFeatureResolver.shouldVerifyDataAfterTest()) {
			if (persistenceExtensionFeatureResolver.hasMultipleDataSources()) {

			} else {
				final ExpectedDataSetProvider dataSetProvider = new ExpectedDataSetProvider(
						this.metadataExtractorInstance.get(), this.dbUnitConfigurationInstance.get());
				for (final DatabaseConnection databaseConnection : this.databaseConnectionProducer.get()
						.getConnections()) {
					this.createExpectedDataSets(databaseConnection,
							dataSetProvider.getDescriptorsDefinedFor(testMethod));
				}
			}
		}

		context.proceed();
	}

   // ------------------------------------------------------------------------------------------------

   private void createDatabaseConnection()
   {

      if (databaseConnectionProducer.get() == null)
      {
         configureDatabaseConnection();
      }

   }

   private void configureDatabaseConnection()
   {
   		final DataSourceMap dataSources = this.dataSourceInstance.get();
		final LinkedList<DatabaseConnection> databaseConnections = new LinkedList<DatabaseConnection>();
		this.dataSoruceTodatabaseConnection = new HashMap<DataSource, DatabaseConnection>();
		for (final DataSource dataSource : dataSources.getDataSources()) {
			final DatabaseConnection databaseConnection = this.buildDatabaseConnectionForDataSource(dataSource);
			/**/
			databaseConnections.add(databaseConnection);
			this.dataSoruceTodatabaseConnection.put(dataSource, databaseConnection);
		}
		this.databaseConnectionProducer.set(new DatabaseConnectionList(databaseConnections));
	}
	
	private DatabaseConnection buildDatabaseConnectionForDataSource(final DataSource dataSource) {
		try {
         final String schema = dbUnitConfigurationInstance.get().getSchema();
         final DatabaseConnection databaseConnection = createDatabaseConnection(dataSource, schema);

			final DatabaseConfig dbUnitConfig = databaseConnection.getConfig();
			dbUnitConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new DefaultDataTypeFactory());

			final Map<String, Object> properties = new DBUnitConfigurationPropertyMapper()
			.map(this.dbUnitConfigurationInstance.get());
			for (final Entry<String, Object> property : properties.entrySet()) {
				dbUnitConfig.setProperty(property.getKey(), property.getValue());
			}
			return databaseConnection;
		} catch (final Exception e) {
			throw new DBUnitInitializationException("Unable to initialize database connection for DBUnit module.", e);
		}
	}

   public DatabaseConnection createDatabaseConnection(final DataSource dataSource, final String schema)
         throws DatabaseUnitException, SQLException
   {
      DatabaseConnection databaseConnection;
      if (schema != null && schema.length() > 0)
      {
         databaseConnection = new DatabaseConnection(dataSource.getConnection(), schema);
      } else
      {
         databaseConnection = new DatabaseConnection(dataSource.getConnection());
      }
      return databaseConnection;
   }

	private void closeDatabaseConnection() {
/*
      try
      {
         final Connection connection = databaseConnectionProducer.get().getConnection();
         if (!connection.isClosed())
         {
            connection.close();
         }
      } catch (Exception e)
      {
         throw new DBUnitConnectionException("Unable to close connection.", e);
      }
*/
   }

	private void createInitialDataSets(final DatabaseConnection databaseConnection,
			final Collection<DataSetResourceDescriptor> dataSetDescriptors) {
		final DataSetRegister dataSetRegister = new DataSetRegister();
		dataSetRegister.setDatabaseConnection(databaseConnection);
		for (final DataSetResourceDescriptor dataSetDescriptor : dataSetDescriptors) {
			dataSetRegister.addInitial(dataSetDescriptor.getContent());
		}
		this.getOrCreateDataSetRegister().getDataSetRegisters().add(dataSetRegister);
	}

	private void createExpectedDataSets(final DatabaseConnection databaseConnection,
			final Collection<DataSetResourceDescriptor> dataSetDescriptors) {
		final DataSetRegister dataSetRegister = new DataSetRegister();
		dataSetRegister.setDatabaseConnection(databaseConnection);
		for (final DataSetResourceDescriptor dataSetDescriptor : dataSetDescriptors) {
			dataSetRegister.addExpected(dataSetDescriptor.getContent());
		}
		this.getOrCreateDataSetRegister().getDataSetRegisters().add(dataSetRegister);
	}

	private DataSetRegisterList getOrCreateDataSetRegister() {
		DataSetRegisterList dataSetRegisterList = this.dataSetRegisterProducer.get();
		if (dataSetRegisterList == null) {
			dataSetRegisterList = new DataSetRegisterList();
			this.dataSetRegisterProducer.set(dataSetRegisterList);
		}
		return dataSetRegisterList;
	}

}
