/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.client
import java.util.{ List => JList }

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.casbin.adapter.JDBCAdapter
import org.casbin.jcasbin.main.Enforcer

import com.mysql.cj.jdbc.MysqlDataSource
/**
 * A Tool for Authorizer implementation.
 *
 * The [[SessionState]] generates the authorizer and authenticator, we use these to check
 * the privileges of a Spark LogicalPlan, which is mapped to hive privilege objects and operation
 * type.
 *
 * [[SparkSession]] with hive catalog implemented has its own instance of [[SessionState]]. I am
 * strongly willing to reuse it, but for the reason that it belongs to an isolated classloader
 * which makes it unreachable for us to visit it in Spark's context classloader. So, when
 * [[ClassCastException]] occurs, we turn off [[IsolatedClientLoader]] to use Spark's builtin
 * Hive client jars to generate a new metastore client to replace the original one, once it is
 * generated, will be reused then.
 *
 */
object AuthzImpl extends Logging {
  def checkPrivileges(
      spark: SparkSession,
      in: JList[HivePrivilegeObject],
      out: JList[HivePrivilegeObject],
      hiveOpType: HiveOperationType): Unit = {

    val user = UserGroupInformation.getCurrentUser.getShortUserName
    
    /* Specify your jdbc configuration
    val url = "jdbc:mysql://localhost:3306/db_name?createDatabaseIfNotExist=true&serverTimezone=UTC";
    val username = "root";
    val password = "password";
    */
    val dataSource = new MysqlDataSource();
    dataSource.setURL(url);
    dataSource.setUser(username);
    dataSource.setPassword(password);

    val a = new JDBCAdapter(dataSource);

    /* specify location of conf file, a sample is under config/
    val e = new Enforcer("rbac_model.conf", a); */
    
    for(obj <- in.asScala)
    {
    	val res = e.enforce(user, obj.getObjectName(), hiveOpType.name());
    	if (res != true) {
    		a.close();
    		logError(
    				s"""
    				|+===============================+
    				||Spark SQL Authorization Failure|
    				||-------------------------------|
    				||$user does not have ${hiveOpType.name()} privilege on ${obj.getObjectName()}
    				||-------------------------------|
    				||Spark SQL Authorization Failure|
    				|+===============================+
    				""".stripMargin)
    		throw new HiveAccessControlException()
    	}     
    }
    
    // Check the permission.

    a.close();

  }
}
