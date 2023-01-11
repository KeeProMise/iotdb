/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.lsm.sstable.fileIO;

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public interface ISSTableInputStream {
  /**
   * Returns the current size of this input.
   *
   * @return The current size of this input, measured in bytes
   * @throws ClosedChannelException If this channel is closed
   * @throws IOException If some other I/O error occurs
   */
  long size() throws IOException;

  /**
   * Returns this input's current position.
   *
   * @return This input's current position, a non-negative integer counting the number of bytes from
   *     the beginning of the input to the current position
   * @throws ClosedChannelException If this input is closed
   * @throws IOException If some other I/O error occurs
   */
  long position() throws IOException;

  /**
   * Sets this input's position.
   *
   * <p>Setting the position to a value that is greater than the input's current size is legal but
   * does not change the size of the IFileInput. A later attempt to read bytes at such a position
   * will immediately return an end-of-file indication.
   *
   * @param newPosition The new position, a non-negative integer counting the number of bytes from
   *     the beginning of the IFileInput
   * @return This TsFileInput
   * @throws ClosedChannelException If this IFileInput is closed
   * @throws IllegalArgumentException If the new position is negative
   * @throws IOException If some other I/O error occurs
   */
  ISSTableInputStream position(long newPosition) throws IOException;

  /**
   * Reads a sequence of bytes from this IFileInput into the given buffer.
   *
   * <p>Bytes are read starting at this IFileInput's current position, and then the position is
   * updated with the number of bytes actually read. Otherwise this method behaves exactly as
   * specified in the {@link ReadableByteChannel} interface.
   */
  int read(ByteBuffer dst) throws IOException;

  /**
   * Reads a sequence of bytes from this IFileInput into the given buffer, starting at the given
   * position.
   *
   * <p>This method works in the same manner as the {@link #read(ByteBuffer)} method, except that
   * bytes are read starting at the given position rather than at the TsFileInput's current
   * position. This method does not modify this IFileInput's position. If the given position is
   * greater than the IFileInput's current size then no bytes are read.
   *
   * @param dst The buffer into which bytes are to be transferred
   * @param position The position at which the transfer is to begin; must be non-negative
   * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the given position is
   *     greater than or equal to the file's current size
   * @throws IllegalArgumentException If the position is negative
   * @throws ClosedChannelException If this IFileInput is closed
   * @throws AsynchronousCloseException If another thread closes this IFileInput while the read
   *     operation is in progress
   * @throws ClosedByInterruptException If another thread interrupts the current thread while the
   *     read operation is in progress, thereby closing the channel and setting the current thread's
   *     interrupt status
   * @throws IOException If some other I/O error occurs
   */
  int read(ByteBuffer dst, long position) throws IOException;

  /** read a byte from the Input. */
  int read() throws IOException;

  /**
   * read an array of byte from the Input.
   *
   * @param b -array of byte
   * @param off -offset of the Input
   * @param len -length
   */
  int read(byte[] b, int off, int len) throws IOException;

  /**
   * read an entry from the file offset
   *
   * @param entry a IEntry object
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     IFileInput
   * @exception EOFException if this input stream reaches the end before reading two bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  void read(IDiskEntry entry, long offset) throws IOException;

  /**
   * read an entry from the file offset
   *
   * @param entry a IEntry object
   * @exception EOFException if this input stream reaches the end before reading two bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  void read(IDiskEntry entry) throws IOException;

  /**
   * Returns the unique {@link java.nio.channels.FileChannel FileChannel} object associated with
   * this file input.
   *
   * <p>The initial {@link java.nio.channels.FileChannel#position() position} of the returned
   * channel will be equal to the number of bytes read from the file so far. Reading bytes from this
   * file input will increment the channel's position. Changing the channel's position, either
   * explicitly or by reading, will change this file input's file position.
   *
   * @return the file channel associated with this file input
   */
  FileChannel wrapAsFileChannel() throws IOException;

  /**
   * Returns the unique {@link java.io.DataInputStream DataInputStream} object associated with this
   * file input.
   *
   * <p>Reading bytes from this DataInputStream will increment the file input's position. Changing
   * the file input's position, either explicitly or by reading, will change this DataInputStream's
   * current position to be read
   *
   * @return the DataInputStream associated with this file input
   */
  DataInputStream wrapAsInputStream() throws IOException;

  /**
   * Closes this file input and releases any system resources associated with the file input.
   *
   * <p>If this stream has an associated channel and stream then the channel and stream is closed as
   * well.
   *
   * @exception IOException if an I/O error occurs.
   */
  void close() throws IOException;

  /** read 4 bytes from the Input and convert it to a integer. */
  int readInt() throws IOException;

  /**
   * See the general contract of the <code>readChar</code> method of <code>DataInput</code>.
   *
   * <p>Bytes for this operation are read from the contained input stream.
   *
   * @return the next two bytes of this input stream, interpreted as a <code>char</code>.
   * @exception EOFException if this input stream reaches the end before reading two bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  char readChar() throws IOException;

  /**
   * See the general contract of the <code>readLong</code> method of <code>DataInput</code>.
   *
   * <p>Bytes for this operation are read from the contained input stream.
   *
   * @return the next eight bytes of this input stream, interpreted as a <code>long</code>.
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  long readLong() throws IOException;

  /**
   * See the general contract of the <code>skipBytes</code> method of <code>DataInput</code>.
   *
   * <p>Bytes for this operation are read from the contained input stream.
   *
   * @param n the number of bytes to be skipped.
   * @return the actual number of bytes skipped.
   * @exception IOException if the contained input stream does not support seek, or the stream has
   *     been closed and the contained input stream does not support reading after close, or another
   *     I/O error occurs.
   */
  int skipBytes(int n) throws IOException;

  /**
   * Get the file path that this file input is reading
   *
   * @return file path
   */
  String getFilePath();
}
