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
import org.apache.arrow.vector.complex.impl.TimeSecReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.TimeSecHolder;
import org.apache.arrow.vector.holders.NullableTimeSecHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * NullableTimeSecVector implements a fixed width (4 bytes) vector of
 * values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class NullableTimeSecVector extends BaseNullableFixedWidthVector {
   private static final org.slf4j.Logger logger =
           org.slf4j.LoggerFactory.getLogger(NullableTimeSecVector.class);
   private static final byte TYPE_WIDTH = 4;
   private final FieldReader reader;

   public NullableTimeSecVector(String name, BufferAllocator allocator) {
      this(name, FieldType.nullable(Types.MinorType.TIMESEC.getType()),
              allocator);
   }

   public NullableTimeSecVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType, TYPE_WIDTH);
      reader = new TimeSecReaderImpl(NullableTimeSecVector.this);
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
      return Types.MinorType.TIMESEC;
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
   public int get(int index) throws IllegalStateException {
      if(isSet(index) == 0) {
         throw new IllegalStateException("Value at index is null");
      }
      return valueBuffer.getInt(index * TYPE_WIDTH);
   }

   /**
    * Get the element at the given index from the vector and
    * sets the state in holder. If element at given index
    * is null, holder.isSet will be zero.
    *
    * @param index   position of element
    */
   public void get(int index, NullableTimeSecHolder holder){
      if(isSet(index) == 0) {
         holder.isSet = 0;
         return;
      }
      holder.isSet = 1;
      holder.value = valueBuffer.getInt(index * TYPE_WIDTH);
   }

   /**
    * Same as {@link #get(int)}.
    *
    * @param index   position of element
    * @return element at given index
    */
   public Integer getObject(int index) {
      if (isSet(index) == 0) {
         return null;
      } else {
         return get(index);
      }
   }

   public void copyFrom(int fromIndex, int thisIndex, NullableTimeSecVector from) {
      if (from.isSet(fromIndex) != 0) {
         set(thisIndex, from.get(fromIndex));
      }
   }

   public void copyFromSafe(int fromIndex, int thisIndex, NullableTimeSecVector from) {
      handleSafe(thisIndex);
      copyFrom(fromIndex, thisIndex, from);
   }


   /******************************************************************
    *                                                                *
    *          vector value setter methods                           *
    *                                                                *
    ******************************************************************/


   private void setValue(int index, int value) {
      valueBuffer.setInt(index * TYPE_WIDTH, value);
   }

   /**
    * Set the element at the given index to the given value.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void set(int index, int value) {
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
   public void set(int index, NullableTimeSecHolder holder) throws IllegalArgumentException {
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
   public void set(int index, TimeSecHolder holder){
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, holder.value);
   }

   /**
    * Same as {@link #set(int, int)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param value   value of element
    */
   public void setSafe(int index, int value) {
      handleSafe(index);
      set(index, value);
   }

   /**
    * Same as {@link #set(int, NullableTimeSecHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void setSafe(int index, NullableTimeSecHolder holder) throws IllegalArgumentException {
      handleSafe(index);
      set(index, holder);
   }

   /**
    * Same as {@link #set(int, TimeSecHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void setSafe(int index, TimeSecHolder holder){
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

   public void set(int index, int isSet, int valueField ) {
      if (isSet > 0) {
         set(index, valueField);
      } else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   public void setSafe(int index, int isSet, int valueField ) {
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
      return new TransferImpl((NullableTimeSecVector)to);
   }

   private class TransferImpl implements TransferPair {
      NullableTimeSecVector to;

      public TransferImpl(String ref, BufferAllocator allocator){
         to = new NullableTimeSecVector(ref, field.getFieldType(), allocator);
      }

      public TransferImpl(NullableTimeSecVector to){
         this.to = to;
      }

      @Override
      public NullableTimeSecVector getTo(){
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
         to.copyFromSafe(fromIndex, toIndex, NullableTimeSecVector.this);
      }
   }
}