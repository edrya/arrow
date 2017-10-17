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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.DecimalReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.TransferPair;

import java.math.BigDecimal;

/**
 * NullableDecimalVector implements a fixed width vector (16 bytes) of
 * decimal values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class NullableDecimalVector extends BaseNullableFixedWidthVector {
   private static final org.slf4j.Logger logger =
           org.slf4j.LoggerFactory.getLogger(NullableDecimalVector.class);
   private static final byte TYPE_WIDTH = 16;
   private final FieldReader reader;

   private final int precision;
   private final int scale;

   public NullableDecimalVector(String name, BufferAllocator allocator,
                                int precision, int scale) {
      this(name, FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(precision, scale)),
              allocator);
   }

   public NullableDecimalVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType, TYPE_WIDTH);
      org.apache.arrow.vector.types.pojo.ArrowType.Decimal arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Decimal)fieldType.getType();
      reader = new DecimalReaderImpl(NullableDecimalVector.this);
      this.precision = arrowType.getPrecision();
      this.scale = arrowType.getScale();
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
      return Types.MinorType.DECIMAL;
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
   public ArrowBuf get(int index) throws IllegalStateException {
      if(isSet(index) == 0) {
         throw new IllegalStateException("Value at index is null");
      }
      return valueBuffer.slice(index * TYPE_WIDTH, TYPE_WIDTH);
   }

   /**
    * Get the element at the given index from the vector and
    * sets the state in holder. If element at given index
    * is null, holder.isSet will be zero.
    *
    * @param index   position of element
    */
   public void get(int index, NullableDecimalHolder holder) {
      if (isSet(index) == 0) {
         holder.isSet = 0;
         return;
      }
      holder.isSet = 1;
      holder.buffer = valueBuffer;
      holder.precision = precision;
      holder.scale = scale;
      holder.start = index * TYPE_WIDTH;
   }

   /**
    * Same as {@link #get(int)}.
    *
    * @param index   position of element
    * @return element at given index
    */
   public BigDecimal getObject(int index) {
      if (isSet(index) == 0) {
         return null;
      } else {
         return DecimalUtility.getBigDecimalFromArrowBuf(valueBuffer, index, scale);
      }
   }

   public void copyFrom(int fromIndex, int thisIndex, NullableDecimalVector from) {
      if (from.isSet(fromIndex) != 0) {
         from.valueBuffer.getBytes(fromIndex * TYPE_WIDTH, valueBuffer,
                 thisIndex * TYPE_WIDTH, TYPE_WIDTH);
      }
   }

   public void copyFromSafe(int fromIndex, int thisIndex, NullableDecimalVector from) {
      handleSafe(thisIndex);
      copyFrom(fromIndex, thisIndex, from);
   }


   /******************************************************************
    *                                                                *
    *          vector value setter methods                           *
    *                                                                *
    ******************************************************************/


   /**
    * Set the element at the given index to the given value.
    *
    * @param index    position of element
    * @param buffer   ArrowBuf containing decimal value.
    */
   public void set(int index, ArrowBuf buffer) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      valueBuffer.setBytes(index * TYPE_WIDTH, buffer, 0, TYPE_WIDTH);
   }

   /**
    * Set the element at the given index to the given value.
    *
    * @param index    position of element
    * @param start    start index of data in the buffer
    * @param buffer   ArrowBuf containing decimal value.
    */
   public void set(int index, int start, ArrowBuf buffer) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      valueBuffer.setBytes(index * TYPE_WIDTH, buffer, start, TYPE_WIDTH);
   }

   /**
    * Set the element at the given index to the given value.
    *
    * @param index   position of element
    * @param value   BigDecimal containing decimal value.
    */
   public void set(int index, BigDecimal value){
      DecimalUtility.checkPrecisionAndScale(value, precision, scale);
      DecimalUtility.writeBigDecimalToArrowBuf(value, valueBuffer, index);
   }

   /**
    * Set the element at the given index to the value set in data holder.
    * If the value in holder is not indicated as set, element in the
    * at the given index will be null.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void set(int index, NullableDecimalHolder holder) throws IllegalArgumentException {
      if(holder.isSet < 0) {
         throw new IllegalArgumentException();
      }
      else if(holder.isSet > 0) {
         BitVectorHelper.setValidityBitToOne(validityBuffer, index);
         valueBuffer.setBytes(index * TYPE_WIDTH, holder.buffer, holder.start, TYPE_WIDTH);
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
   public void set(int index, DecimalHolder holder){
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      valueBuffer.setBytes(index * TYPE_WIDTH, holder.buffer, holder.start, TYPE_WIDTH);
   }

   /**
    * Same as {@link #set(int, ArrowBuf)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param buffer  ArrowBuf containing decimal value.
    */
   public void setSafe(int index, ArrowBuf buffer) {
      handleSafe(index);
      set(index, buffer);
   }

   /**
    * Same as {@link #set(int, int, ArrowBuf)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index    position of element
    * @param start    start index of data in the buffer
    * @param buffer   ArrowBuf containing decimal value.
    */
   public void setSafe(int index, int start, ArrowBuf buffer) {
      handleSafe(index);
      set(index, start, buffer);
   }

   /**
    * Same as {@link #set(int, BigDecimal)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param value   BigDecimal containing decimal value.
    */
   public void setSafe(int index, BigDecimal value){
      handleSafe(index);
      set(index, value);
   }

   /**
    * Same as {@link #set(int, NullableDecimalHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void setSafe(int index, NullableDecimalHolder holder) throws IllegalArgumentException {
      handleSafe(index);
      set(index, holder);
   }

   /**
    * Same as {@link #set(int, DecimalHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void setSafe(int index, DecimalHolder holder){
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

   public void set(int index, int isSet, int start, ArrowBuf buffer) {
      if (isSet > 0) {
         set(index, start, buffer);
      } else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   public void setSafe(int index, int isSet, int start, ArrowBuf buffer) {
      handleSafe(index);
      set(index, isSet, start, buffer);
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
      return new TransferImpl((NullableDecimalVector)to);
   }

   private class TransferImpl implements TransferPair {
      NullableDecimalVector to;

      public TransferImpl(String ref, BufferAllocator allocator){
         to = new NullableDecimalVector(ref, allocator, NullableDecimalVector.this.precision,
                 NullableDecimalVector.this.scale);
      }

      public TransferImpl(NullableDecimalVector to){
         this.to = to;
      }

      @Override
      public NullableDecimalVector getTo(){
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
         to.copyFromSafe(fromIndex, toIndex, NullableDecimalVector.this);
      }
   }
}