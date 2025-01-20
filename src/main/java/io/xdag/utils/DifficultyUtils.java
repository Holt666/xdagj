package io.xdag.utils;

import io.xdag.core.v2.Block;
import io.xdag.core.v2.BlockHeader;
import io.xdag.store.XdagStore;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DifficultyUtils {
    
    /**
     * Convert difficulty to target nbits
     * @param difficulty The difficulty value
     * @return 4-byte nbits value
     */
    public static byte[] encodeNbits(BigInteger difficulty) {
        byte[] result = new byte[4];
        
        // Calculate target = (2^256-1) / difficulty
        BigInteger max = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
        BigInteger target = max.divide(difficulty);
        
        // Convert to compact format
        byte[] targetBytes = target.toByteArray();
        int nSize = targetBytes.length;
        
        result[0] = (byte) nSize;
        if (nSize >= 3) {
            result[1] = targetBytes[0];
            result[2] = targetBytes[1];
            result[3] = targetBytes[2];
        } else {
            // Handle smaller numbers
            result[1] = nSize > 0 ? targetBytes[0] : 0;
            result[2] = nSize > 1 ? targetBytes[1] : 0;
            result[3] = nSize > 2 ? targetBytes[2] : 0;
        }
        
        return result;
    }

    /**
     * Convert nbits back to difficulty
     * @param nbits The 4-byte nbits value
     * @return The difficulty value
     */
    public static BigInteger decodeDifficulty(byte[] nbits) {
        if (nbits.length != 4) {
            throw new IllegalArgumentException("nbits must be 4 bytes");
        }

        int nSize = nbits[0] & 0xFF;
        long nWord = ((nbits[1] & 0xFFL) << 16) |
                    ((nbits[2] & 0xFFL) << 8) |
                    (nbits[3] & 0xFFL);

        BigInteger target;
        if (nSize <= 3) {
            target = BigInteger.valueOf(nWord).shiftRight(8 * (3 - nSize));
        } else {
            target = BigInteger.valueOf(nWord).shiftLeft(8 * (nSize - 3));
        }

        // difficulty = (2^256-1) / target
        BigInteger max = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
        return max.divide(target);
    }

    /**
     * Compare difficulties of two blocks
     * @param nbits1 The nbits of first block
     * @param nbits2 The nbits of second block
     * @return positive if block1 is harder, negative if block2 is harder, 0 if equal
     */
    public static int compareDifficulty(byte[] nbits1, byte[] nbits2) {
        BigInteger diff1 = decodeDifficulty(nbits1);
        BigInteger diff2 = decodeDifficulty(nbits2);
        return diff1.compareTo(diff2);
    }

    /**
     * Check if block1 has higher difficulty than block2
     * @param nbits1 The nbits of first block
     * @param nbits2 The nbits of second block
     * @return true if block1 has higher difficulty
     */
    public static boolean isMoreDifficult(byte[] nbits1, byte[] nbits2) {
        return compareDifficulty(nbits1, nbits2) > 0;
    }

    // Constants for difficulty adjustment
    public static final BigInteger MINIMUM_DIFFICULTY = BigInteger.valueOf(1000000);
    public static final int TARGET_BLOCK_TIME = 64; // Target seconds per block
    public static final int BLOCKS_PER_HOUR = 3600 / TARGET_BLOCK_TIME; // ~56.25 blocks per hour
    public static final int TARGET_BLOCK_COUNT = BLOCKS_PER_HOUR; // Target number of blocks per hour
    public static final long DIFFICULTY_ADJUSTMENT_INTERVAL = 60 * 60 * 1000; // 1 hour in milliseconds
    
    // 根据出块速度偏离目标的程度来动态调整难度
    private static double calculateAdjustmentFactor(double actualBlocksPerHour) {
        // 计算实际出块速度与目标的比率
        double ratio = actualBlocksPerHour / TARGET_BLOCK_COUNT;
        
        // 使用分段函数进行难度调整
        if (ratio > 1.5) { // 出块速度过快 (>150%)
            // 使用立方函数快速增加难度
            return Math.pow(ratio, 3.0);
        } else if (ratio > 1.1) { // 出块速度略快 (110%-150%)
            // 使用平方函数适度增加难度
            return Math.pow(ratio, 2.0);
        } else if (ratio < 0.5) { // 出块速度过慢 (<50%)
            // 使用平方根函数快速降低难度
            return Math.sqrt(ratio);
        } else if (ratio < 0.9) { // 出块速度略慢 (50%-90%)
            // 使用立方根函数缓慢降低难度
            return Math.pow(ratio, 1.0/3.0);
        } else { // 出块速度接近目标 (90%-110%)
            // 使用线性调整，但调整幅度减小
            return 1.0 + (ratio - 1.0) * 0.5;
        }
    }

    /**
     * Calculate new difficulty based on recent block history
     * @param currentDifficulty Current difficulty
     * @param recentBlocks List of recent blocks in the last adjustment interval
     * @param currentTime Current timestamp
     * @return New difficulty value
     */
    public static BigInteger calculateNextDifficulty(BigInteger currentDifficulty,
                                                     List<Block> recentBlocks, long currentTime) {
        if (recentBlocks.isEmpty()) {
            return MINIMUM_DIFFICULTY;
        }

        // 获取时间跨度
        long oldestTime = recentBlocks.stream()
                .map(Block::getHeader)
                .mapToLong(BlockHeader::getTimestamp)
                .min()
                .orElse(currentTime);
        long timespan = currentTime - oldestTime;

        // 计算每小时出块数
        if (timespan > 0) {
            double blocksPerHour = (double) recentBlocks.size() * DIFFICULTY_ADJUSTMENT_INTERVAL / timespan;
            double adjustmentFactor = calculateAdjustmentFactor(blocksPerHour);
            
            // 计算新难度
            BigInteger newDifficulty;
            if (blocksPerHour > TARGET_BLOCK_COUNT) {
                // 出块过快，增加难度
                newDifficulty = currentDifficulty.multiply(
                        BigInteger.valueOf((long)(adjustmentFactor * 100)))
                        .divide(BigInteger.valueOf(100));
            } else {
                // 出块过慢，降低难度
                newDifficulty = currentDifficulty.multiply(BigInteger.valueOf(100))
                        .divide(BigInteger.valueOf((long)(adjustmentFactor * 100)));
            }
            
            return newDifficulty.max(MINIMUM_DIFFICULTY);
        }
        
        return currentDifficulty;
    }

    /**
     * Check if a block's difficulty is high enough to be considered for inclusion
     * @param blockNbits The block's nbits
     * @param recentBlocks List of recent blocks to compare against
     * @return true if the block's difficulty is among the top N blocks
     */
    public static boolean isHighEnoughDifficulty(byte[] blockNbits, List<BlockHeader> recentBlocks) {
        if (recentBlocks.isEmpty()) {
            return decodeDifficulty(blockNbits).compareTo(MINIMUM_DIFFICULTY) >= 0;
        }

        // Sort blocks by difficulty (highest first)
        recentBlocks.sort((b1, b2) -> -compareDifficulty(b1.getNbits(), b2.getNbits()));

        // Get the minimum difficulty from recent blocks
        byte[] minAcceptedNbits = recentBlocks.get(0).getNbits();
        BigInteger minAcceptedDifficulty = decodeDifficulty(minAcceptedNbits);
        BigInteger blockDifficulty = decodeDifficulty(blockNbits);

        // Block difficulty should be at least 50% of the highest difficulty
        return blockDifficulty.multiply(BigInteger.valueOf(2))
                .compareTo(minAcceptedDifficulty) >= 0;
    }

    /**
     * Get initial difficulty for network start
     * @return Initial difficulty value
     */
    public static BigInteger getInitialDifficulty() {
        return MINIMUM_DIFFICULTY;
    }

    /**
     * Calculate cumulative work difficulty from genesis to current block
     * @param block Current block
     * @param store XdagStore instance to get block history
     * @return Cumulative difficulty as BigInteger
     */
    public static BigInteger calculateCumulativeDifficulty(Block block, XdagStore store) {
        BigInteger totalDifficulty = BigInteger.ZERO;
        Block currentBlock = block;
        
        while (currentBlock != null) {
            // Add current block's difficulty
            totalDifficulty = totalDifficulty.add(getBlockDifficulty(currentBlock));
            
            // Get parent block
            byte[] parentHash = currentBlock.getParentHash();
            if (Arrays.equals(parentHash, new byte[32])) { // Genesis block check
                break;
            }
            currentBlock = store.getBlockByHash(parentHash);
        }
        
        return totalDifficulty;
    }

    /**
     * Compare chains by cumulative difficulty
     * @param block1 Tip of first chain
     * @param block2 Tip of second chain
     * @param store XdagStore instance
     * @return positive if chain1 is harder, negative if chain2 is harder, 0 if equal
     */
    public static int compareChainDifficulty(Block block1, Block block2, XdagStore store) {
        BigInteger diff1 = calculateCumulativeDifficulty(block1, store);
        BigInteger diff2 = calculateCumulativeDifficulty(block2, store);
        return diff1.compareTo(diff2);
    }

    /**
     * Find the fork point between two chains
     * @param block1 Tip of first chain
     * @param block2 Tip of second chain
     * @param store XdagStore instance
     * @return The fork point block, or null if no common ancestor found
     */
    public static Block findForkPoint(Block block1, Block block2, XdagStore store) {
        Set<byte[]> chain1Blocks = new HashSet<>();
        Block current = block1;
        
        // Collect all block hashes in first chain
        while (current != null) {
            chain1Blocks.add(current.getHash());
            byte[] parentHash = current.getParentHash();
            if (Arrays.equals(parentHash, new byte[32])) {
                break;
            }
            current = store.getBlockByHash(parentHash);
        }
        
        // Find first common block in second chain
        current = block2;
        while (current != null) {
            if (chain1Blocks.contains(current.getHash())) {
                return current;
            }
            byte[] parentHash = current.getParentHash();
            if (Arrays.equals(parentHash, new byte[32])) {
                break;
            }
            current = store.getBlockByHash(parentHash);
        }
        
        return null;
    }

    /**
     * Get blocks between two points in a chain
     * @param from Starting block (newer)
     * @param to Ending block (older)
     * @param store XdagStore instance
     * @return List of blocks in order from newer to older, excluding 'to' block
     */
    public static List<Block> getBlocksBetween(Block from, Block to, XdagStore store) {
        List<Block> blocks = new ArrayList<>();
        Block current = from;
        
        while (current != null && !Arrays.equals(current.getHash(), to.getHash())) {
            blocks.add(current);
            current = store.getBlockByHash(current.getParentHash());
        }
        
        return blocks;
    }

    /**
     * Check if a chain is valid (all blocks connected and difficulties correct)
     * @param tip Chain tip block
     * @param store XdagStore instance
     * @return true if chain is valid
     */
    public static boolean isValidChain(Block tip, XdagStore store) {
        Block current = tip;
        BigInteger previousDifficulty = null;
        
        while (current != null) {
            // Check minimum difficulty
            BigInteger difficulty = getBlockDifficulty(current);
            if (difficulty.compareTo(MINIMUM_DIFFICULTY) < 0) {
                return false;
            }
            
            // Check difficulty adjustment if not first block
            if (previousDifficulty != null) {
                // Add any specific difficulty adjustment rules here
            }
            
            previousDifficulty = difficulty;
            byte[] parentHash = current.getParentHash();
            
            if (Arrays.equals(parentHash, new byte[32])) {
                break;
            }
            current = store.getBlockByHash(parentHash);
        }
        
        return true;
    }
} 