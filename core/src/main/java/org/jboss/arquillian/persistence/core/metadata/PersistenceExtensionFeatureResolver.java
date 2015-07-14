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
package org.jboss.arquillian.persistence.core.metadata;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import org.jboss.arquillian.persistence.Cleanup;
import org.jboss.arquillian.persistence.CleanupStrategy;
import org.jboss.arquillian.persistence.CleanupUsingScript;
import org.jboss.arquillian.persistence.DataSeedStrategy;
import org.jboss.arquillian.persistence.DataSource;
import org.jboss.arquillian.persistence.DataSourceWithData;
import org.jboss.arquillian.persistence.DataSourcesWithData;
import org.jboss.arquillian.persistence.SeedDataUsing;
import org.jboss.arquillian.persistence.TestExecutionPhase;
import org.jboss.arquillian.persistence.core.configuration.PersistenceConfiguration;
import org.jboss.arquillian.persistence.core.exception.DataSourceNotDefinedException;
import org.jboss.arquillian.persistence.core.util.Strings;

/**
 *
 * @author <a href="mailto:bartosz.majsak@gmail.com">Bartosz Majsak</a>
 *
 */
public class PersistenceExtensionFeatureResolver
{

   private final PersistenceConfiguration configuration;

   private final MetadataExtractor metadataExtractor;

   private final Method testMethod;

   public PersistenceExtensionFeatureResolver(Method testMethod, MetadataExtractor metadataExtractor, PersistenceConfiguration configuration)
   {
      this.metadataExtractor = metadataExtractor;
      this.configuration = configuration;
      this.testMethod = testMethod;
   }

   // ---------------------------------------------------------------------------------------------------
   // Public API methods
   // ---------------------------------------------------------------------------------------------------

   public boolean shouldCreateSchema()
   {
      return metadataExtractor.createSchema().isDefinedOnClassLevel();
   }

    public boolean shouldSeedData() {
        return this.metadataExtractor.usingDataSet().isDefinedOnClassLevel()
                || this.metadataExtractor.usingDataSet().isDefinedOn(this.testMethod)
                || this.metadataExtractor.dataSourcesWithData().isDefinedOn(this.testMethod);
    }

    public boolean hasMultipleDataSources() {
        return this.metadataExtractor.dataSourcesWithData().isDefinedOn(this.testMethod);
    }

   public boolean shouldCustomScriptBeAppliedBeforeTestRequested()
   {
      return metadataExtractor.applyScriptBefore().isDefinedOnClassLevel()
            || metadataExtractor.applyScriptBefore().isDefinedOn(testMethod);
   }

   public boolean shouldCustomScriptBeAppliedAfterTestRequested()
   {
      return metadataExtractor.applyScriptAfter().isDefinedOnClassLevel()
            || metadataExtractor.applyScriptAfter().isDefinedOn(testMethod);
   }

   public boolean shouldVerifyDataAfterTest()
   {
      return metadataExtractor.shouldMatchDataSet().isDefinedOnClassLevel()
            || metadataExtractor.shouldMatchDataSet().isDefinedOn(testMethod);
   }

   public TestExecutionPhase getCleanupTestPhase()
   {
      final Cleanup cleanupAnnotation = metadataExtractor.cleanup().fetchUsingFirst(testMethod);

      TestExecutionPhase phase = configuration.getDefaultCleanupPhase();
      if (cleanupAnnotation != null && !TestExecutionPhase.DEFAULT.equals(cleanupAnnotation.phase()))
      {
         phase = cleanupAnnotation.phase();
      }

      return phase;
   }

   public CleanupStrategy getCleanupStrategy()
   {
      final Cleanup cleanup = metadataExtractor.cleanup().fetchUsingFirst(testMethod);
      if (cleanup == null || CleanupStrategy.DEFAULT.equals(cleanup.strategy()))
      {
         return configuration.getDefaultCleanupStrategy();
      }
      return cleanup.strategy();
   }

   public DataSeedStrategy getDataSeedStrategy()
   {
      final SeedDataUsing dataSeedingStrategy = metadataExtractor.dataSeedStrategy().fetchUsingFirst(testMethod);
      if (dataSeedingStrategy == null || DataSeedStrategy.DEFAULT.equals(dataSeedingStrategy.value()))
      {
         return configuration.getDefaultDataSeedStrategy();
      }
      return dataSeedingStrategy.value();
   }

   public boolean shouldCleanup()
   {
      final CleanupUsingScript cleanupUsingScriptAnnotation = metadataExtractor.cleanupUsingScript().fetchUsingFirst(testMethod);
      return cleanupUsingScriptAnnotation == null || TestExecutionPhase.NONE.equals(cleanupUsingScriptAnnotation.phase());
   }

   public boolean shouldCleanupBefore()
   {
      return shouldCleanup() && TestExecutionPhase.BEFORE.equals(getCleanupTestPhase());
   }

   public boolean shouldCleanupAfter()
   {
      return shouldCleanup() && TestExecutionPhase.AFTER.equals(getCleanupTestPhase());
   }

    public List<String> getDataSourceName() {
        final LinkedList<String> dataSourcesNames = new LinkedList<String>();

        final DataSource dataSourceAnnotation = this.metadataExtractor.dataSource().fetchUsingFirst(this.testMethod);
        if (dataSourceAnnotation != null) {
            final String dataSourceName = dataSourceAnnotation.value();
            dataSourcesNames.add(dataSourceName);
        }
        final DataSourcesWithData dataSourcesWithData = this.metadataExtractor.dataSourcesWithData().fetchUsingFirst(
                this.testMethod);
        if (dataSourcesWithData != null) {
            for (final DataSourceWithData dataSourceWithData : dataSourcesWithData.value()) {
                final String dataSourceName = dataSourceWithData.source().value();
                dataSourcesNames.add(dataSourceName);
            }
        }
        if ((dataSourceAnnotation == null) && (dataSourcesWithData == null)) {
            String dataSource = "";

            if (this.configuration.isDefaultDataSourceDefined()) {
                dataSource = this.configuration.getDefaultDataSource();
            }
            if (Strings.isEmpty(dataSource)) {
                throw new DataSourceNotDefinedException(
                        "DataSource not defined! Please declare in arquillian.xml or by using @DataSource annotation.");
            }
            dataSourcesNames.add(dataSource);
        }

        return dataSourcesNames;
    }

}
