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
import org.apache.arrow.vector.complex.impl.BitReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

/**
 * NullableBitVector implements a fixed width (1 bit) vector of
 * boolean values which could be null. Each value in the vector corresponds
 * to a single bit in the underlying data stream backing the vector.
 */
public class NullableBitVector extends BaseNullableFixedWidthVector {
   private static final org.slf4j.Logger logger =
           org.slf4j.LoggerFactory.getLogger(NullableBitVector.class);
   private final FieldReader reader;

   public NullableBitVector(String name, BufferAllocator allocator) {
      this(name, FieldType.nullable(Types.MinorType.BIT.getType()),
              allocator);
   }

   public NullableBitVector(String name, FieldType fieldType, BufferAllocator allocator) {
      super(name, allocator, fieldType, (byte)0);
      reader = new BitReaderImpl(NullableBitVector.this);
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
      return Types.MinorType.BIT;
   }

   @Override
   public void setInitialCapacity(int valueCount) {
      final int size = getSizeFromCount(valueCount);
      if (size > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
      }
      valueAllocationSizeInBytes = size;
      validityAllocationSizeInBytes = size;
   }

   @Override
   public int getValueCapacity(){
      return (int)(validityBuffer.capacity() * 8L);
   }

   @Override
   public int getBufferSizeFor(final int count) {
      if (count == 0) { return 0; }
      return 2 * getSizeFromCount(count);
   }

   @Override
   public int getBufferSize() {
     return getBufferSizeFor(valueCount);
   }

   public void splitAndTransferTo(int startIndex, int length,
                                  BaseNullableFixedWidthVector target) {
      compareTypes(target, "splitAndTransferTo");
      target.clear();
      target.validityBuffer = splitAndTransferBuffer(startIndex, length, target,
                                 validityBuffer, target.validityBuffer);
      target.valueBuffer = splitAndTransferBuffer(startIndex, length, target,
                                 valueBuffer, target.valueBuffer);

      target.setValueCount(length);
   }

   private ArrowBuf splitAndTransferBuffer(int startIndex, int length,
                                               BaseNullableFixedWidthVector target,
                                               ArrowBuf sourceBuffer, ArrowBuf destBuffer) {
      assert startIndex + length <= valueCount;
      int firstByteSource = BitVectorHelper.byteIndex(startIndex);
      int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
      int byteSizeTarget = getSizeFromCount(length);
      int offset = startIndex % 8;

      if (length > 0) {
         if (offset == 0) {
            /* slice */
            if (destBuffer != null) {
               destBuffer.release();
            }
            destBuffer = destBuffer.slice(firstByteSource, byteSizeTarget);
            destBuffer.retain(1);
         }
         else {
            /* Copy data
             * When the first bit starts from the middle of a byte (offset != 0),
             * copy data from src BitVector.
             * Each byte in the target is composed by a part in i-th byte,
             * another part in (i+1)-th byte.
             */
            destBuffer = allocator.buffer(byteSizeTarget);
            destBuffer.readerIndex(0);
            destBuffer.setZero(0, destBuffer.capacity());

            for (int i = 0; i < byteSizeTarget - 1; i++) {
               byte b1 = BitVectorHelper.getBitsFromCurrentByte(sourceBuffer, firstByteSource + i, offset);
               byte b2 = BitVectorHelper.getBitsFromNextByte(sourceBuffer, firstByteSource + i + 1, offset);

               destBuffer.setByte(i, (b1 + b2));
            }

            /* Copying the last piece is done in the following manner:
             * if the source vector has 1 or more bytes remaining, we copy
             * the last piece as a byte formed by shifting data
             * from the current byte and the next byte.
             *
             * if the source vector has no more bytes remaining
             * (we are at the last byte), we copy the last piece as a byte
             * by shifting data from the current byte.
             */
            if((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
               byte b1 = BitVectorHelper.getBitsFromCurrentByte(sourceBuffer,
                       firstByteSource + byteSizeTarget - 1, offset);
               byte b2 = BitVectorHelper.getBitsFromNextByte(sourceBuffer,
                       firstByteSource + byteSizeTarget, offset);

               destBuffer.setByte(byteSizeTarget - 1, b1 + b2);
            }
            else {
               byte b1 = BitVectorHelper.getBitsFromCurrentByte(sourceBuffer,
                       firstByteSource + byteSizeTarget - 1, offset);
               destBuffer.setByte(byteSizeTarget - 1, b1);
            }
         }
      }

      return destBuffer;
   }


   /******************************************************************
    *                                                                *
    *          vector value retrieval methods                        *
    *                                                                *
    ******************************************************************/

   private int getBit(int index) {
      final int byteIndex = index >> 3;
      final byte b = valueBuffer.getByte(byteIndex);
      final int bitIndex = index & 7;
      return Long.bitCount(b & (1L << bitIndex));
   }

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
      return getBit(index);
   }

   /**
    * Get the element at the given index from the vector and
    * sets the state in holder. If element at given index
    * is null, holder.isSet will be zero.
    *
    * @param index   position of element
    */
   public void get(int index, NullableBitHolder holder){
      if(isSet(index) == 0) {
         holder.isSet = 0;
         return;
      }
      holder.isSet = 1;
      holder.value = getBit(index);
   }

   /**
    * Same as {@link #get(int)}.
    *
    * @param index   position of element
    * @return element at given index
    */
   public Boolean getObject(int index) {
      if (isSet(index) == 0) {
         return null;
      } else {
         return new Boolean (getBit(index) != 0);
      }
   }

   public void copyFrom(int fromIndex, int thisIndex, NullableBitVector from) {
      if (from.isSet(fromIndex) != 0) {
         set(thisIndex, from.get(fromIndex));
      }
   }

   public void copyFromSafe(int fromIndex, int thisIndex, NullableBitVector from) {
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
    * @param index   position of element
    * @param value   value of element
    */
   public void set(int index, int value) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      if (value != 0) {
         BitVectorHelper.setValidityBitToOne(valueBuffer, index);
      } else {
         BitVectorHelper.setValidityBit(valueBuffer, index, 0);
      }
   }

   /**
    * Set the element at the given index to the value set in data holder.
    * If the value in holder is not indicated as set, element in the
    * at the given index will be null.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void set(int index, NullableBitHolder holder) throws IllegalArgumentException {
      if(holder.isSet < 0) {
         throw new IllegalArgumentException();
      }
      else if(holder.isSet > 0) {
         BitVectorHelper.setValidityBitToOne(validityBuffer, index);
         if (holder.value != 0) {
            BitVectorHelper.setValidityBitToOne(valueBuffer, index);
         } else {
            BitVectorHelper.setValidityBit(valueBuffer, index, 0);
         }
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
   public void set(int index, BitHolder holder) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      if (holder.value != 0) {
         BitVectorHelper.setValidityBitToOne(valueBuffer, index);
      } else {
         BitVectorHelper.setValidityBit(valueBuffer, index, 0);
      }
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
    * Same as {@link #set(int, NullableBitHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  nullable data holder for value of element
    */
   public void setSafe(int index, NullableBitHolder holder) throws IllegalArgumentException {
      handleSafe(index);
      set(index, holder);
   }

   /**
    * Same as {@link #set(int, BitHolder)} except that it handles the
    * case when index is greater than or equal to existing
    * value capacity {@link #getValueCapacity()}.
    *
    * @param index   position of element
    * @param holder  data holder for value of element
    */
   public void setSafe(int index, BitHolder holder){
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

   public void set(int index, int isSet, int value) {
      if (isSet > 0) {
         set(index, value);
      } else {
         BitVectorHelper.setValidityBit(validityBuffer, index, 0);
      }
   }

   public void setSafe(int index, int isSet, int value) {
      handleSafe(index);
      set(index, isSet, value);
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
      return new TransferImpl((NullableBitVector)to);
   }

   private class TransferImpl implements TransferPair {
      NullableBitVector to;

      public TransferImpl(String ref, BufferAllocator allocator){
         to = new NullableBitVector(ref, field.getFieldType(), allocator);
      }

      public TransferImpl(NullableBitVector to){
         this.to = to;
      }

      @Override
      public NullableBitVector getTo(){
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
         to.copyFromSafe(fromIndex, toIndex, NullableBitVector.this);
      }
   }
}