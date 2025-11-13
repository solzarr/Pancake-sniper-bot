import { ethers } from 'ethers';
import * as dotenv from 'dotenv';

// Load env vars (e.g., PRIVATE_KEY, SNIPER_CONTRACT_ADDRESS)
dotenv.config();

// BSC Mainnet addresses
const ADDRESSES = {
  WBNB: '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
  FACTORY: '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73' as const, // PancakeSwap V2 Factory
  ROUTER: '0x10ED43C718714eb63d5aA57B78B54704E256024E' as const, // PancakeSwap V2 Router
} as const;

// Sniper Contract address (REQUIRED - deploy first and set in .env)
const SNIPER_CONTRACT_ADDRESS = process.env.SNIPER_CONTRACT_ADDRESS;
if (!SNIPER_CONTRACT_ADDRESS) {
  throw new Error('SNIPER_CONTRACT_ADDRESS not set in .env file! Deploy the contract first.');
}

// Primary QuickNode WebSocket URL (for critical operations - event monitoring, swaps)
const PROVIDER_URL = process.env.WS_PROVIDER_URL!;

// Multiple BSC RPC endpoints for multi-broadcast (faster propagation)
const BSC_RPC_ENDPOINTS = [
  'https://bsc-dataseed1.binance.org',
  'https://bsc-dataseed2.binance.org',
  'https://bsc-dataseed3.binance.org',
  'https://bsc-dataseed4.binance.org',
  'https://bsc-dataseed1.defibit.io',
  'https://bsc-dataseed2.defibit.io',
  'https://bsc-dataseed1.ninicoin.io',
  'https://bsc-dataseed2.ninicoin.io',
];

// Private key from env (REQUIRED for swaps)
const PRIVATE_KEY = process.env.PRIVATE_KEY;
if (!PRIVATE_KEY) {
  throw new Error('PRIVATE_KEY not set in .env file!');
}

// Minimal ABI for Factory (unchanged)
const FACTORY_ABI = [
  'event PairCreated(address indexed token0, address indexed token1, address pair, uint)',
  'function allPairsLength() view returns (uint)',
] as const;

// ABI for SniperContract buy function
const SNIPER_ABI = [
  'function buy(address token, uint256 amountIn, uint256 deadlineOffset) external',
] as const;

// Initialize multiple RPC providers for broadcasting
const rpcProviders: ethers.JsonRpcProvider[] = BSC_RPC_ENDPOINTS.map(
  url => new ethers.JsonRpcProvider(url)
);

// OPTIMIZED: Multi-RPC broadcast - Submit signed tx to multiple RPCs simultaneously
async function submitRawTx(rawTx: string, timeoutMs = 5000): Promise<string> {
  const start = Date.now();
  
  // Add 0x prefix if missing
  const txWithPrefix = rawTx.startsWith('0x') ? rawTx : '0x' + rawTx;
  
  console.log('📤 Broadcasting to', rpcProviders.length, 'RPC endpoints...');
  
  // Submit to all RPCs simultaneously
  const submissions = rpcProviders.map(async (provider, index) => {
    try {
      const response = await Promise.race([
        provider.broadcastTransaction(txWithPrefix),
        new Promise<never>((_, reject) => 
          setTimeout(() => reject(new Error('Timeout')), timeoutMs)
        )
      ]);
      
      if (!response || !response.hash) {
        throw new Error('No transaction hash returned');
      }
      
      console.log(`  ✅ RPC ${index + 1} accepted (${Date.now() - start}ms)`);
      return { success: true, hash: response.hash, provider: index };
    } catch (err: any) {
      console.log(`  ⚠️  RPC ${index + 1} failed: ${err.message.substring(0, 50)}`);
      return { success: false, error: err.message, provider: index };
    }
  });

  // Wait for first successful response
  const results = await Promise.allSettled(submissions);
  
  // Find first successful submission
  for (const result of results) {
    if (result.status === 'fulfilled' && result.value.success && result.value.hash) {
      const txHash = result.value.hash;
      console.log(`✅ Transaction broadcast successful: ${txHash}`);
      
      // Let other submissions complete in background (don't await)
      Promise.allSettled(submissions).then(() => {
        const successCount = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
        console.log(`📊 Final: ${successCount}/${rpcProviders.length} RPCs accepted tx`);
      });
      
      return txHash;
    }
  }
  
  // All failed
  throw new Error('All RPC endpoints failed to broadcast transaction');
}

async function main() {
  console.log('🚀 Starting Multi-RPC sniper...');
  console.log(`📡 Configured ${rpcProviders.length} RPC endpoints for broadcasting`);

  // Connect to BSC via WebSocket for events (read-only)
  const provider = new ethers.WebSocketProvider(PROVIDER_URL);
  console.log('Connected to BSC WebSocket provider (event monitoring)...');

  // Initialize Factory for events (read-only)
  const factory = new ethers.Contract(ADDRESSES.FACTORY, FACTORY_ABI, provider);

  // Initialize SniperContract with signer for swaps
  const wallet = new ethers.Wallet(PRIVATE_KEY!, provider);
  const sniperContract = new ethers.Contract(SNIPER_CONTRACT_ADDRESS!, SNIPER_ABI, wallet);
  console.log(`Wallet address: ${wallet.address}`);
  console.log(`Sniper Contract: ${SNIPER_CONTRACT_ADDRESS}`);

  // NEW: Manual nonce management for speed
  let currentNonce = await provider.getTransactionCount(wallet.address, 'pending');

  // OPTIMIZED: Lightning-fast swap function (BNB → New Token via SniperContract) - Multi-RPC Broadcast
  async function executeSwap(newToken: string, amountInBNB: string, deadlineMinutes = 3, competitiveGas = false) {
    const start = Date.now();

    try {
      console.log(`⚡ SWAP: ${amountInBNB} BNB → ${newToken} (via SniperContract)`);

      const amountIn = ethers.parseEther(amountInBNB);
      const deadlineOffset = BigInt(deadlineMinutes * 60); // deadlineOffset in seconds

      // OPTIMIZATION: Dynamic gas pricing for same-block attempts
      const gasLimit = 300000n; // Adjusted for contract call
      let gasPrice: bigint;
      
      if (competitiveGas) {
        // AGGRESSIVE: For mempool-detected txs (try for same block)
        const feeData = await provider.getFeeData();
        const networkGas = feeData.gasPrice || ethers.parseUnits('3', 'gwei');
        gasPrice = (networkGas * 150n) / 100n; // 50% higher than network to frontrun
        console.log(`  💰 Using COMPETITIVE gas: ${ethers.formatUnits(gasPrice, 'gwei')} Gwei`);
      } else {
        // NORMAL: For event-detected txs (already mined, no rush)
        gasPrice = ethers.parseUnits('3', 'gwei'); // 3 Gwei is typical for BSC
      }

      // Use manual nonce management
      const nonce = currentNonce;

      // Populate and sign transaction (all in one flow) - NO value, contract uses its balance
      const txRequest = await sniperContract.buy.populateTransaction(
        newToken,
        amountIn,
        deadlineOffset,
        {
          gasLimit,
          gasPrice,
          nonce,
          chainId: 56n,
        }
      );

      const signedTx = await wallet.signTransaction(txRequest);
      const rawTx = signedTx.slice(2); // Remove 0x prefix

      const signTime = Date.now() - start;

      // OPTIMIZED: Submit via Multi-RPC broadcast for maximum speed
      const txHash = await submitRawTx(rawTx, 5000); // 5s timeout per RPC

      const submitTime = Date.now() - start;
      console.log(`✅ Submitted (sign: ${signTime}ms, submit: ${submitTime}ms): ${txHash}`);

      // Increment nonce immediately after successful submission
      currentNonce++;

      // Wait for confirmation (async in background - don't block)
      provider.waitForTransaction(txHash, 1, 45000) // 45s timeout, 1 confirmation
        .then(receipt => {
          if (receipt) {
            console.log(`🎯 Confirmed in block ${receipt.blockNumber} (${Date.now() - start}ms total)`);
          }
        })
        .catch(err => {
          console.error(`⚠️  Confirmation timeout for ${txHash}: ${err.message}`);
        });

      return { txHash, success: true, submitTime };

    } catch (error: any) {
      console.error(`❌ Swap failed (${Date.now() - start}ms): ${error.message}`);

      // If nonce error, resync nonce from network
      if (error.message.includes('nonce') || error.message.includes('already known')) {
        console.log('Resyncing nonce...');
        currentNonce = await provider.getTransactionCount(wallet.address, 'pending');
      }

      return { success: false, error: error.message, time: Date.now() - start };
    }
  }

  // OPTIMIZED: Subscribe to PairCreated events with fast filtering
  factory.on('PairCreated', async (token0: string, token1: string, pair: string, event: any) => {
    const eventStart = Date.now();

    // OPTIMIZATION: Fast lowercase comparison with pre-computed constant
    const token0Lower = token0.toLowerCase();
    const token1Lower = token1.toLowerCase();
    const wbnbLower = ADDRESSES.WBNB.toLowerCase();

    // Quick filter: Must be WBNB pair
    if (token0Lower !== wbnbLower && token1Lower !== wbnbLower) {
      return; // Skip non-WBNB pairs instantly
    }

    const newToken = token0Lower === wbnbLower ? token1 : token0;

    // Target found! Log and execute
    console.log(`\n🚨 TARGET DETECTED (${Date.now() - eventStart}ms from event)`);
    console.log(`  Pair: ${pair}`);
    console.log(`  Token: ${newToken}`);
    console.log(`  Block: ${event.log?.blockNumber || 'unknown'}`);
    console.log(`  Time: ${new Date().toISOString()}`);
    console.log('─'.repeat(60));

    // CRITICAL: Execute swap immediately (normal gas - pair already created)
    executeSwap(newToken, '0.0001', 3, false).catch(err => {
      console.error('Swap execution error:', err.message);
    });
  });

  // OPTIMIZED: Nonce refresh every 30s to stay synced
  setInterval(async () => {
    const latestNonce = await provider.getTransactionCount(wallet.address, 'pending');
    if (latestNonce > currentNonce) {
      console.log(`⚙️  Nonce updated: ${currentNonce} → ${latestNonce}`);
      currentNonce = latestNonce;
    }
  }, 30000);

  // Graceful shutdown
  process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down...');
    provider.destroy();
    process.exit(0);
  });
}

main().catch(console.error);