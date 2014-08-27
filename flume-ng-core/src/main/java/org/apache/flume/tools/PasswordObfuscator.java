/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.flume.FlumeException;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Use AES 128bit, CTR Mode - with default key and iv
 */

public class PasswordObfuscator {
  // 16-byte (128-bit)  key
  static final byte[] keyBytes = new byte[] { (byte) 0x00, (byte) 0x1, (byte) 0x2,
          (byte) 0x3, (byte) 0x4, (byte) 0x5, (byte) 0x6,
          (byte) 0x7, (byte) 0x8, (byte) 0x9, (byte) 0xa,
          (byte) 0xb, 0xc, (byte) 0xd, (byte) 0xe, (byte) 0xf };

  // Initialization vector 16 bytes
  static final byte[] iv = new byte[] { (byte) 0xf, (byte) 0xe, (byte) 0xd,
          (byte) 0xc, (byte) 0xb, (byte) 0xa, (byte) 0x9,
          (byte) 0x8, (byte) 0x7, (byte) 0x6, (byte) 0x5,
          (byte) 0x4, (byte) 0x3, (byte) 0x2, (byte) 0x1, (byte) 0x0 };
  // Initialization
  static final SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
  static final IvParameterSpec ivSpec = new IvParameterSpec(iv);

  public static final String TYPE_TEXT = "TEXT";
  public static final String TYPE_AES = "AES";


  /**
   * Encodes the originalText and writes it to a file
   * @param originalText the text to be encoded
   * @return the encoded string
   */
  public static byte[] encode(String originalText)  {
    try {
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
      return cipher.doFinal(originalText.getBytes());
    } catch (Exception e) {
      throw new FlumeException("AES Encryption Failed", e);
    }
  }

  /**
   * Encodes the originalText and writes it to a file
   * @param originalText the text to be encoded
   * @param outputFile the file where the encoded bytes will be written
   * @throws java.io.IOException if could not write to file
   */
  public static void encodeToFile(String originalText, String outputFile)
          throws IOException {
      File output = new File(outputFile);
      byte[] cipherBytes = encode(originalText);
      FileUtils.writeByteArrayToFile(output, cipherBytes);
  }


  public static String decode(byte[] encoded, String charEncoding) {
    try {
      Cipher decipher = Cipher.getInstance("AES/CTR/NoPadding");
      decipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
      byte[] plain = decipher.doFinal(encoded);
      return new String(plain, charEncoding);
    } catch (Exception e) {
      throw new FlumeException("AES Encryption Failed: " + e.getMessage(), e);
    }
  }

  /**
   * Decodes the password from a file that was created using encodeToFile
   * @param inputFile the file where the encoded password is stored
   * @param charEncoding encoding to convert decoded bytes from file to the returned String object
   * @return the decoded string
   */
  private static String decodeFromFile(File inputFile, String charEncoding) {
    try {
      byte[] cipherBytes = FileUtils.readFileToByteArray(inputFile);
      return decode(cipherBytes,charEncoding);
    } catch (Exception e) {
      throw new FlumeException("AES Encryption Failed: " + e.getMessage(), e);
    }
  }

  /**
   *
   * @param args  needs --outfile
   */
  public static void main(String[] args) throws IOException, ParseException {
    Options options = new Options();

    Option option = new Option(null, "outfile", true, "the file in which to store the password");
    option.setRequired(true);
    options.addOption(option);

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);

    String outputFile = commandLine.getOptionValue("outfile");

    System.out.println("Enter the password : ");
    String password = new String( System.console().readPassword() );
    System.out.println("Verify password    : ");
    if( !password.equals( new String(System.console().readPassword()) ) ) {
      System.err.println("Passwords do not match. Please try again");
      return;
    }

    try {
      encodeToFile(password, outputFile);
      System.out.println();
      System.out.println("Password has been stored in file : " + outputFile);
    } catch (IOException e) {
      System.err.println("Unable to write to output file : " + outputFile);
    }

  }

  /**
   * Decodes the password that was encoded using encode()
   * @param passwordFile
   * @param passwordFileType Can be "AES" or "TEXT" (case in-sensitive)
   * @return the decoded string
   */
  public static String readPasswordFromFile(String passwordFile, String passwordFileType) {
    try {
      File pfile = new File(passwordFile);
      if( passwordFileType.equalsIgnoreCase(TYPE_TEXT) ) {
          return FileUtils.readLines(pfile, "UTF-8").get(0);
      } else if ( passwordFileType.equalsIgnoreCase(TYPE_AES) ) {
        return PasswordObfuscator.decodeFromFile(pfile, "UTF-8");
      } else {
        throw new IllegalArgumentException("Unsupported password file format");
      }
    } catch (IOException e) {
      throw new FlumeException(e.getMessage(), e);
    }
  }

}
