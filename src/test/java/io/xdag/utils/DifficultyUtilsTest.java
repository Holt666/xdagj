package io.xdag.utils;

import io.xdag.core.v2.Block;
import io.xdag.core.v2.BlockHeader;
import io.xdag.crypto.randomx.RandomXCache;
import io.xdag.crypto.randomx.RandomXFlag;
import io.xdag.crypto.randomx.RandomXTemplate;
import io.xdag.crypto.randomx.RandomXUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.nio.ByteBuffer;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DifficultyUtilsTest {

    @Test
    public void testEncodeAndDecodeDifficulty() {
        // Test various difficulty values
        BigInteger[] testValues = {
                BigInteger.valueOf(1000000),  // Minimum difficulty
                BigInteger.valueOf(5000000),
                new BigInteger("1000000000000")  // Large difficulty
        };

        for (BigInteger difficulty : testValues) {
            byte[] nbits = DifficultyUtils.encodeNbits(difficulty);
            BigInteger decoded = DifficultyUtils.decodeDifficulty(nbits);
            
            // Allow small rounding difference due to compact format
            BigInteger difference = difficulty.subtract(decoded).abs();
            assertTrue("Difference too large for difficulty " + difficulty,
                    difference.compareTo(difficulty.divide(BigInteger.valueOf(100))) < 0);
        }
    }

    @Test
    public void testCompareDifficulty() {
        BigInteger diff1 = BigInteger.valueOf(2000000);
        BigInteger diff2 = BigInteger.valueOf(1000000);

        byte[] nbits1 = DifficultyUtils.encodeNbits(diff1);
        byte[] nbits2 = DifficultyUtils.encodeNbits(diff2);

        assertTrue(DifficultyUtils.isMoreDifficult(nbits1, nbits2));
        assertFalse(DifficultyUtils.isMoreDifficult(nbits2, nbits1));
        assertEquals(0, DifficultyUtils.compareDifficulty(nbits1, nbits1));
    }

    @Test
    public void testCalculateNextDifficulty() {
        long currentTime = System.currentTimeMillis();
        List<Block> recentBlocks = new ArrayList<>();
        
        // Create mock blocks
        for (int i = 0; i < 15; i++) {
            Block block = createMockBlock(currentTime - (i * 1000), 1500000);
            recentBlocks.add(block);
        }

        // Test with too many blocks (should increase difficulty)
        BigInteger newDifficulty = DifficultyUtils.calculateNextDifficulty(
                BigInteger.valueOf(1000000),
                recentBlocks,
                currentTime
        );
        assertTrue(newDifficulty.compareTo(BigInteger.valueOf(1000000)) > 0);

        // Test with empty block list (should return minimum difficulty)
        assertEquals(
                DifficultyUtils.MINIMUM_DIFFICULTY,
                DifficultyUtils.calculateNextDifficulty(
                        BigInteger.valueOf(1000000),
                        new ArrayList<>(),
                        currentTime
                )
        );
    }

    @Test
    public void testIsHighEnoughDifficulty() {
        List<BlockHeader> recentBlocks = new ArrayList<>();
        
        // Create mock blocks with different difficulties
        for (int i = 0; i < 15; i++) {
            BlockHeader header = createMockHeader(1000000 + i * 100000);
            recentBlocks.add(header);
        }

        // Test block with high enough difficulty
        byte[] highDifficultyNbits = DifficultyUtils.encodeNbits(BigInteger.valueOf(2000000));
        assertTrue(DifficultyUtils.isHighEnoughDifficulty(highDifficultyNbits, recentBlocks));

        // Test block with too low difficulty
        byte[] lowDifficultyNbits = DifficultyUtils.encodeNbits(BigInteger.valueOf(1000000));
        assertFalse(DifficultyUtils.isHighEnoughDifficulty(lowDifficultyNbits, recentBlocks));

        // Test with empty block list (should only check minimum difficulty)
        assertTrue(DifficultyUtils.isHighEnoughDifficulty(
                highDifficultyNbits,
                new ArrayList<>()
        ));
    }

    @Test
    public void testGetInitialDifficulty() {
        assertEquals(
                DifficultyUtils.MINIMUM_DIFFICULTY,
                DifficultyUtils.getInitialDifficulty()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNbitsLength() {
        // Test invalid nbits length
        byte[] invalidNbits = new byte[]{1, 2, 3}; // Only 3 bytes
        DifficultyUtils.decodeDifficulty(invalidNbits);
    }

    @Test
    public void testDifficultyAdjustmentFor30SecondBlocks() {
        long currentTime = System.currentTimeMillis();
        List<Block> recentBlocks = new ArrayList<>();
        
        // Create mock blocks with 30 second intervals
        // 一小时应该有120个区块 (3600/30=120)
        for (int i = 0; i < 120; i++) {
            Block block = createMockBlock(
                    currentTime - (i * 30000), // 30秒间隔
                    1500000
            );
            recentBlocks.add(block);
        }

        // 当前难度
        BigInteger currentDifficulty = BigInteger.valueOf(1500000);
        
        // 计算新难度
        BigInteger newDifficulty = DifficultyUtils.calculateNextDifficulty(
                currentDifficulty,
                recentBlocks,
                currentTime
        );

        // 打印难度变化信息（用于调试）
        System.out.println("Original difficulty: " + currentDifficulty);
        System.out.println("New difficulty: " + newDifficulty);
        System.out.println("Block count: " + recentBlocks.size());
        System.out.println("Time span: " + (currentTime - recentBlocks.get(recentBlocks.size()-1).getHeader().getTimestamp()) + "ms");
        System.out.println("Blocks per hour: " + (recentBlocks.size() * 3600000.0 / (currentTime - recentBlocks.get(recentBlocks.size()-1).getHeader().getTimestamp())));
        
        // 由于出块速度是目标速度的12倍 (每30秒一个，目标是每6分钟10个)
        // 根据当前的调整因子1.2，难度应该增加20%
        BigInteger expectedMinDifficulty = currentDifficulty.multiply(BigInteger.valueOf(120)).divide(BigInteger.valueOf(100));
        assertTrue("Difficulty should increase by at least 20%",
                newDifficulty.compareTo(expectedMinDifficulty) >= 0);
        
        // 验证难度不会低于最小难度
        assertTrue("Difficulty should not be less than minimum",
                newDifficulty.compareTo(DifficultyUtils.MINIMUM_DIFFICULTY) >= 0);
    }

    @Test
    public void testMiningDifficulty() {
        long currentTime = System.currentTimeMillis();
        List<Block> recentBlocks = new ArrayList<>();
        
        // 模拟矿机每10秒出一个块的情况
        // 一小时应该有360个区块 (3600/10=360)
        for (int i = 0; i < 360; i++) {
            Block block = createMockBlock(
                    currentTime - (i * 10000), // 10秒间隔
                    1500000
            );
            recentBlocks.add(block);
        }

        BigInteger currentDifficulty = BigInteger.valueOf(1500000);
        BigInteger newDifficulty = DifficultyUtils.calculateNextDifficulty(
                currentDifficulty,
                recentBlocks,
                currentTime
        );

        // 打印挖矿相关信息
        System.out.println("=== Mining Difficulty Test ===");
        System.out.println("Original difficulty: " + currentDifficulty);
        System.out.println("New difficulty: " + newDifficulty);
        System.out.println("Block count in last hour: " + recentBlocks.size());
        double avgBlockTime = (double)(currentTime - recentBlocks.get(recentBlocks.size()-1).getHeader().getTimestamp()) / recentBlocks.size() / 1000;
        System.out.println("Average block time: " + String.format("%.2f", avgBlockTime) + " seconds");
        double blocksPerHour = recentBlocks.size() * 3600000.0 / (currentTime - recentBlocks.get(recentBlocks.size()-1).getHeader().getTimestamp());
        System.out.println("Blocks per hour: " + String.format("%.2f", blocksPerHour));
        System.out.println("Difficulty increase factor: " + String.format("%.2fx", newDifficulty.doubleValue() / currentDifficulty.doubleValue()));
        
        // 由于出块速度是目标速度的6.4倍 (64/10=6.4)
        // 使用立方调整因子，期望的最小增长应该是 6.4^3 ≈ 262.144
        double expectedMinIncrease = Math.pow(6.4, 3.0);
        BigInteger expectedMinDifficulty = currentDifficulty.multiply(BigInteger.valueOf((long)(expectedMinIncrease * 100))).divide(BigInteger.valueOf(100));
        
        assertTrue("Mining difficulty should increase significantly",
                newDifficulty.compareTo(expectedMinDifficulty) >= 0);
    }

    @Test
    public void testSHA256Mining() {
        long currentTime = System.currentTimeMillis();
        List<Block> recentBlocks = new ArrayList<>();
        BigInteger currentDifficulty = BigInteger.valueOf(1500000);
        
        // 模拟挖矿过程
        int totalBlocks = 100; // 测试100个区块
        long startTime = currentTime - (totalBlocks * DifficultyUtils.TARGET_BLOCK_TIME * 1000);
        
        for (int i = 0; i < totalBlocks; i++) {
            // 创建区块数据
            byte[] blockData = new byte[32];
            new Random().nextBytes(blockData);
            
            // 记录开始挖矿时间
            long miningStartTime = System.currentTimeMillis();
            long nonce = 0;
            byte[] hash;
            
            // 计算目标难度值
            BigInteger target = BigInteger.valueOf(2).pow(256).divide(currentDifficulty);
            
            // 挖矿循环
            while (true) {
                // 组合数据: blockData + nonce
                byte[] data = new byte[blockData.length + 8];
                System.arraycopy(blockData, 0, data, 0, blockData.length);
                ByteBuffer.wrap(data, blockData.length, 8).putLong(nonce);
                
                // 计算SHA256
                try {
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    hash = digest.digest(data);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
                
                // 检查是否满足难度要求
                BigInteger hashValue = new BigInteger(1, hash);
                if (hashValue.compareTo(target) <= 0) {
                    break;
                }
                
                nonce++;
            }
            
            // 记录结束时间和实际用时
            long miningEndTime = System.currentTimeMillis();
            long blockTime = miningEndTime - miningStartTime;
            
            // 创建并添加区块
            Block block = createMockBlock(startTime + (i * DifficultyUtils.TARGET_BLOCK_TIME * 1000),
                    currentDifficulty.longValue());
            recentBlocks.add(block);
            
            // 每10个区块调整一次难度
            if (i > 0 && i % 10 == 0) {
                currentDifficulty = DifficultyUtils.calculateNextDifficulty(
                        currentDifficulty,
                        recentBlocks,
                        miningEndTime
                );
                
                // 打印挖矿统计信息
                System.out.println("\n=== Mining Statistics (Block " + i + ") ===");
                System.out.println("Current Difficulty: " + currentDifficulty);
                System.out.println("Average Block Time: " + (blockTime / 1000.0) + " seconds");
                System.out.println("Nonce Found: " + nonce);
                System.out.println("Hash: " + bytesToHex(hash));
            }
        }
        
        // 验证最终难度是否在合理范围内
        double avgBlockTime = calculateAverageBlockTime(recentBlocks);
        System.out.println("\n=== Final Mining Statistics ===");
        System.out.println("Final Difficulty: " + currentDifficulty);
        System.out.println("Average Block Time: " + avgBlockTime + " seconds");
        
        // 验证平均出块时间是否接近目标时间
        assertTrue("Average block time should be within 50% of target",
                Math.abs(avgBlockTime - DifficultyUtils.TARGET_BLOCK_TIME) < DifficultyUtils.TARGET_BLOCK_TIME * 0.5);
    }
    
    private double calculateAverageBlockTime(List<Block> blocks) {
        if (blocks.size() < 2) return 0;
        
        long totalTime = blocks.get(0).getHeader().getTimestamp() - 
                blocks.get(blocks.size() - 1).getHeader().getTimestamp();
        return (double) totalTime / (blocks.size() - 1) / 1000.0;
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    // Helper methods for creating mock objects
    private Block createMockBlock(long timestamp, long difficulty) {
        Block block = mock(Block.class);
        BlockHeader header = createMockHeader(difficulty);
        when(block.getHeader()).thenReturn(header);
        when(header.getTimestamp()).thenReturn(timestamp);
        return block;
    }

    private BlockHeader createMockHeader(long difficulty) {
        BlockHeader header = mock(BlockHeader.class);
        when(header.getNbits()).thenReturn(DifficultyUtils.encodeNbits(BigInteger.valueOf(difficulty)));
        return header;
    }

    @Test
    public void testRandomXMining() {
        long currentTime = System.currentTimeMillis();
        List<Block> recentBlocks = new ArrayList<>();
        BigInteger currentDifficulty = BigInteger.valueOf(1500000);

        byte[] key = new byte[32];
        Random random = new Random();
        random.nextBytes(key);

        Set<RandomXFlag> flags = RandomXUtils.getFlagsSet();
        RandomXCache cache = new RandomXCache(flags);
        cache.init(key);
        
        // 使用RandomXTemplate.builder()创建实例
        try (RandomXTemplate randomx = RandomXTemplate.builder().flags(flags).cache(cache)
                .build()) {
            
            // 初始化RandomX

            randomx.init();
            
            try {
                // 等待RandomX初始化完成
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("RandomX initialization interrupted", e);
            }
            
            // 模拟挖矿过程
            int totalBlocks = 50; // 测试50个区块
            long startTime = currentTime - (totalBlocks * DifficultyUtils.TARGET_BLOCK_TIME * 1000);
            
            for (int i = 0; i < totalBlocks; i++) {
                // 创建区块数据
                byte[] blockData = new byte[32];
                new Random().nextBytes(blockData);
                
                // 记录开始挖矿时间
                long miningStartTime = System.currentTimeMillis();
                long nonce = 0;
                byte[] hash;
                
                // 计算目标难度值
                BigInteger target = BigInteger.valueOf(2).pow(256).divide(currentDifficulty);
                
                // 挖矿循环
                while (true) {
                    // 组合数据: blockData + nonce
                    byte[] data = new byte[blockData.length + 8];
                    System.arraycopy(blockData, 0, data, 0, blockData.length);
                    ByteBuffer.wrap(data, blockData.length, 8).putLong(nonce);
                    
                    // 使用RandomXTemplate计算哈希
                    hash = randomx.calculateHash(data);
                    
                    // 检查是否满足难度要求
                    BigInteger hashValue = new BigInteger(1, hash);
                    if (hashValue.compareTo(target) <= 0) {
                        break;
                    }
                    
                    nonce++;
                    
                    // 每1000次尝试检查一次是否超时
                    if (nonce % 1000 == 0) {
                        long currentMiningTime = System.currentTimeMillis() - miningStartTime;
                        if (currentMiningTime > 30000) { // 30秒超时
                            System.out.println("Mining timeout for block " + i + ", adjusting difficulty");
                            currentDifficulty = currentDifficulty.multiply(BigInteger.valueOf(90)).divide(BigInteger.valueOf(100));
                            break;
                        }
                    }
                }
                
                // 记录结束时间和实际用时
                long miningEndTime = System.currentTimeMillis();
                long blockTime = miningEndTime - miningStartTime;
                
                // 创建并添加区块
                Block block = createMockBlock(startTime + (i * DifficultyUtils.TARGET_BLOCK_TIME * 1000),
                        currentDifficulty.longValue());
                recentBlocks.add(block);
                
                // 每5个区块调整一次难度
                if (i > 0 && i % 5 == 0) {
                    currentDifficulty = DifficultyUtils.calculateNextDifficulty(
                            currentDifficulty,
                            recentBlocks,
                            miningEndTime
                    );
                    
                    // 打印挖矿统计信息
                    System.out.println("\n=== RandomX Mining Statistics (Block " + i + ") ===");
                    System.out.println("Current Difficulty: " + currentDifficulty);
                    System.out.println("Block Mining Time: " + (blockTime / 1000.0) + " seconds");
                    System.out.println("Nonce Found: " + nonce);
                    System.out.println("Hash: " + bytesToHex(hash));
                    
                    // 计算并打印平均出块时间
                    double avgBlockTime = calculateAverageBlockTime(recentBlocks);
                    System.out.println("Average Block Time: " + avgBlockTime + " seconds");
                    System.out.println("Hash Rate: " + String.format("%.2f", nonce / (blockTime / 1000.0)) + " H/s");
                }
            }
            
            // 验证最终难度是否在合理范围内
            double avgBlockTime = calculateAverageBlockTime(recentBlocks);
            System.out.println("\n=== Final RandomX Mining Statistics ===");
            System.out.println("Final Difficulty: " + currentDifficulty);
            System.out.println("Average Block Time: " + avgBlockTime + " seconds");
            
            // 验证平均出块时间是否接近目标时间（允许较大误差范围）
            assertTrue("Average block time should be within reasonable range",
                    Math.abs(avgBlockTime - DifficultyUtils.TARGET_BLOCK_TIME) < DifficultyUtils.TARGET_BLOCK_TIME);
        }
    }
} 