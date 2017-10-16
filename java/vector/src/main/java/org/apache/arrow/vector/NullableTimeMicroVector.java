/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.TimeMicroReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.TimeMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeMicroHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;

/**
 * NullableTimeMicroVector implements a fixed width vector (8 bytes) of
 * integer values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class NullableTimeMicroVector extends BaseNullableFixedWidthVector {
   private static final org.slf4j.Logger logger =
           org.slf4j.LoggerFactory.getLogger(NullableTimeMicroVector.class);
   private static final byte TYPE_WIDTH = 8;
   private final FieldReader reader;

   public NullableTimeMicroVector(String name, BufferAllocator allocator) {
      this(name, FieldType.nullable(Types.MinorType.TIMEMICRO.getType()),
              allocator);
   }

   public NullableTimeMicroVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType, TYPE_WIDTH);
      reader = new TimeMicroReaderImpl(NullableTimeMicroVector.this);
   }

   @Override
   protected org.slf4j.Logger getLogger() {
      return logger;
   }

   @Override
   public FieldReader getReader(){
      return reader;
   }

   @Override
   public Types.MinorType getMinorType() {
      return Types.MinorType.TIMEMICRO;
   }


   /******************************************************************
    *                                                                *
    *          vector value retrieval methods                        *
    *                                                                *
    ******************************************************************/


   /**
    * Get the element at the given index from the vector.
    *
    * @param index   position of element
    * @return element at given index
    */
   public long get(int index) throws IllegalStateException {
      if(isSet(index) == 0) {
         throw new IllegalStateException("Value at index is null");
      }
      return valueBuffer.getLong(index * TYPE_WIDTH);
   }

   /**
    * Get the element at the given index from the vector and
    * sets the state in holder. If element at given index
    * is null, holder.isSet will be zero.
    *
    * @param index   position of element
    */
   public void get(int index, NullableTimeMicroHolder holder){
      if(isSet(index) == 0) {
         holder.isSet = 0;
         return;
      }
      holder.isSet = 1;
      holder.value = valueBuffer.getLong(index * TYPE_WIDTH);
   }

   /**
    * Same as {@link #get(int)}.
    *
    * @param index   position of element
    * @return element at given index
    */
   public Long getObject(int index) {
      if (isSet(index) == 0) {
         return null;
      } else {
         return get(index);
      }
   }

   public void copyFrom(int fromIndex, int thisIndex, NullableTimeMicroVector from) {
      if (from.isSet(fromIndex) != 0) {
         set(thisIndex, from.get(fromIndex));
      }
   }

   public void copyFromSafe(int fromIndex, int thisIndex, NullableTimeMicroVector from) {
      handleSafe(thisIndex);
      copyFrom(fromIndex, thisIndex, from);
   }


   /******************************************************************
    *                                                                *
    *          vector value setter methods                           *
    *                                                                *
    ******************************************************************/


   private void setValue(int index, long value) {
      valueBuffer.setLong(index * TYPE_WIDTH, value);
   }

   /**
    * Set the element at the given index to the given value.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void set(int index, long value) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, value);
   }

   /**
    * Set the element at the given index to the value set in data holder.
    * If the value in holder is not indicated as set, element in the
    * at the given index will be null.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void set(int index, NullableTimeMicroHolder holder) throws IllegalArgumentException {
      if(holder.isSet < 0) {
         throw new IllegalArgumentException();
      }
      else if(holder.isSet > 0) {
         BitVectorHelper.setValidityBitToOne(validityBuffer, index);
         setValue(index, holder.value);
      }
      else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   /**
    * Set the element at the given index to the value set in data holder.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void set(int index, TimeMicroHolder holder){
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, holder.value);
   }

   /**
    * Same as {@link #set(int, long)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void setSafe(int index, long value) {
      handleSafe(index);
      set(index, value);
   }

   /**
    * Same as {@link #set(int, NullableTimeMicroHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void setSafe(int index, NullableTimeMicroHolder holder) throws IllegalArgumentException {
      handleSafe(index);
      set(index, holder);
   }

   /**
    * Same as {@link #set(int, TimeMicroHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void setSafe(int index, TimeMicroHolder holder){
      handleSafe(index);
      set(index, holder);
   }

   /**
    * Set the element at the given index to null.
    *
    * @param index   position of element
    */
   public void setNull(int index){
      handleSafe(index);
      /* not really needed to set the bit to 0 as long as
       * the buffer always starts from 0.
       */
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
   }

   public void set(int index, int isSet, long valueField ) {
      if (isSet > 0) {
         set(index, valueField);
      } else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   public void setSafe(int index, int isSet, long valueField ) {
      handleSafe(index);
      set(index, isSet, valueField);
   }


   /******************************************************************
    *                                                                *
    *                      vector transfer                           *
    *                                                                *
    ******************************************************************/


   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator){
      return new TransferImpl(ref, allocator);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector to) {
      return new TransferImpl((NullableTimeMicroVector)to);
   }

   private class TransferImpl implements TransferPair {
      NullableTimeMicroVector to;

      public TransferImpl(String ref, BufferAllocator allocator){
         to = new NullableTimeMicroVector(ref, field.getFieldType(), allocator);
      }

      public TransferImpl(NullableTimeMicroVector to){
         this.to = to;
      }

      @Override
      public NullableTimeMicroVector getTo(){
         return to;
      }

      @Override
      public void transfer(){
         transferTo(to);
      }

      @Override
      public void splitAndTransfer(int startIndex, int length) {
         splitAndTransferTo(startIndex, length, to);
      }

      @Override
      public void copyValueSafe(int fromIndex, int toIndex) {
         to.copyFromSafe(fromIndex, toIndex, NullableTimeMicroVector.this);
      }
   }
}