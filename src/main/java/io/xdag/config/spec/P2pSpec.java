/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020-2030 The XdagJ Developers
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.xdag.config.spec;

import java.util.Set;

/**
 * Interface for P2P network configuration.
 */
public interface P2pSpec {

    /**
     * Get the set of authorized addresses
     */
    Set<String> getAuthorizedAddresses();

    /**
     * Check if an address is authorized
     */
    boolean isAddressAuthorized(String address);

    /**
     * Load authorized addresses from registry file
     */
    boolean loadRegistry();

    /**
     * Get the URL for downloading the registry.dat file
     * @return String containing the registry URL, or null if using default
     */
    String getRegistryUrl();

    /**
     * Get the default registry URL
     * @return String containing the default registry URL
     */
    String getDefaultRegistryUrl();

    /**
     * Get the maximum size of the registry file in bytes
     * @return int maximum size in bytes
     */
    int getRegistryMaxSize();

    /**
     * Get the maximum number of active nodes allowed
     * @return int maximum number of active nodes
     */
    int getMaxActiveNodes();

    /**
     * Get the HTTP timeout for registry download in milliseconds
     * @return int timeout in milliseconds
     */
    int getRegistryHttpTimeout();

    /**
     * Get the registry refresh interval in hours
     * @return long hours between registry refreshes
     */
    long getRegistryRefreshInterval();

    /**
     * Get the node discovery interval in minutes
     * @return long minutes between node discoveries
     */
    long getNodeDiscoveryInterval();

    /**
     * Get the node cleanup interval in minutes
     * @return long minutes between node cleanups
     */
    long getNodeCleanupInterval();

    /**
     * Get the maximum number of registry download retries
     * @return int maximum number of retries
     */
    int getRegistryMaxRetries();

    /**
     * Get the registry download retry delay in seconds
     * @return long seconds to wait between retries
     */
    long getRegistryRetryDelay();

    /**
     * Get the node discovery backoff multiplier
     * @return double multiplier for exponential backoff
     */
    double getNodeDiscoveryBackoffMultiplier();

    /**
     * Get the maximum node discovery backoff in minutes
     * @return long maximum minutes to wait between discoveries
     */
    long getNodeDiscoveryMaxBackoff();

} 