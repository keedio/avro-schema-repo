/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.repo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestValidatingSubject {
  private static final String SUB = "sub";
  private static final String FOO = "foo";
  private static final String BAR = "bar";
  private static final String BAZ = "baz";

  private InMemoryRepository repo;
  private Subject sub;

  @Before
  public void setUpRepository() {
    repo = new InMemoryRepository();
    sub = repo.register(SUB, null);
    Assert.assertNotNull("failed to register subject: " + SUB, sub);
  }

  @After
  public void tearDownRepository() {
    repo = null;
    sub = null;
  }

  @Test
  public void testSuccessfulValidation() throws SchemaValidationException {
    
    Subject alwaysValid = alwaysValid(sub);
    
    SchemaEntry foo = alwaysValid.registerIfLatest(FOO,  null);
    Assert.assertNotNull("failed to register schema", foo);
    SchemaEntry bar = alwaysValid.registerIfLatest(BAR, foo);
    Assert.assertNotNull("failed to register schema", bar);
    SchemaEntry none = alwaysValid.registerIfLatest("nothing", null);
    Assert.assertNull(none);
    SchemaEntry baz = alwaysValid.register(BAZ);
    Assert.assertNotNull("failed to register schema", baz);
    
  }
  
  @Test
  public void testValidatorConstruction() {
    Assert.assertNull("Must pass null through Subject.validateWith()",
        alwaysValid(null));
    Assert.assertEquals("Must pass subject through when validator is null",
        sub, Subject.validateWith(sub, null));
  }

  @Test(expected=SchemaValidationException.class)
  public void testCannotRegister() throws SchemaValidationException {
    Subject neverValid = neverValid(sub);
    neverValid.register(FOO);
  }

  @Test(expected=SchemaValidationException.class)
  public void testCannotRegisterIfLatest() throws SchemaValidationException {
    Subject neverValid = neverValid(sub);
    neverValid.registerIfLatest(FOO, null);
  }
  
  private Subject neverValid(Subject sub) {
    return Subject.validateWith(sub, new Validator(){
      @Override
      public void validate(String schemaToValidate,
          Iterable<SchemaEntry> schemasInOrder)
          throws SchemaValidationException {
       throw new SchemaValidationException("no sir!");
      }
    });
  }
  
  private Subject alwaysValid(Subject sub) {
    return Subject.validateWith(sub, new Validator() {
      @Override
      public void validate(String schemaToValidate,
          Iterable<SchemaEntry> schemasInOrder)
          throws SchemaValidationException {
       return; 
      }
    });
  }

}
