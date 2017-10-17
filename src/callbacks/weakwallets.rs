use std::fs::{self, File};
use std::path::PathBuf;
use std::io::{BufWriter, Write};
use std::string::String;

use clap::{Arg, ArgMatches, App, SubCommand};

use callbacks::Callback;
use errors::{OpError, OpResult};

use blockchain::proto::tx::{Tx, TxInput, EvaluatedTxOut};
use blockchain::parser::types::CoinType;
use blockchain::proto::block::Block;
use blockchain::proto::Hashed;
use blockchain::utils;


/// Dumps the whole blockchain into csv files
pub struct WeakWallets {
    // Each structure gets stored in a seperate csv file
    dump_folder:    PathBuf,
    ww_writer:   BufWriter<File>,

    start_height:   usize,
    end_height:     usize,
    tx_count:       u64,
    in_count:       u64,
    out_count:      u64
}

impl WeakWallets {
    fn create_writer(cap: usize, path: PathBuf) -> OpResult<BufWriter<File>> {
        let file = match File::create(&path) {
            Ok(f) => f,
            Err(err) => return Err(OpError::from(err))
        };
        Ok(BufWriter::with_capacity(cap, file))
    }

    fn block_r_arr(block: &Block) -> Vec<String> {
        let mut r_arr: Vec<String> = Vec::new();
        for tx in &block.txs {
            for input in &tx.value.inputs {
                if input.script_sig.len() > 74 {
                    println!("{} {}", utils::arr_to_hex(&input.script_sig), utils::arr_to_hex(&input.script_sig).len());
                    let tmp_sig = utils::arr_to_hex(&input.script_sig);
                    r_arr.push(tmp_sig[10..64].to_string());
                }
            }
        }
        return r_arr;
    }

    fn repeat_r(sig: String, arr: &Vec<String>) -> bool {
        if sig.len() > 74 {
            let n: i8 = 0;
            for r in arr {
                if sig[10..64].to_string() == r.to_string() {
                    let n = n + 1;
                    if n > 1 {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl Callback for WeakWallets {

    fn build_subcommand<'a, 'b>() -> App<'a, 'b> where Self: Sized {
        SubCommand::with_name("weakwallets")
            .about("Dumps the weak wallets into CSV files")
            .version("0.1")
            .author("windy <xfstudio@qq.com>")
            .arg(Arg::with_name("dump-folder")
                .help("Folder to store csv files")
                .index(1)
                .required(true))
    }

    fn new(matches: &ArgMatches) -> OpResult<Self> where Self: Sized {
        let ref dump_folder = PathBuf::from(matches.value_of("dump-folder").unwrap()); // Save to unwrap
        match (|| -> OpResult<Self> {
            let cap = 4000000;
            let cb = WeakWallets {
                dump_folder:    PathBuf::from(dump_folder),
                ww_writer:   try!(WeakWallets::create_writer(cap, dump_folder.join("weak_wallets.csv.tmp"))),
                start_height: 0, end_height: 0, tx_count: 0, in_count: 0, out_count: 0
            };
            Ok(cb)
        })() {
            Ok(s) => return Ok(s),
            Err(e) => return Err(
                tag_err!(e, "Couldn't initialize weakwallets with folder: `{}`", dump_folder
                        .as_path()
                        .display()))
        }
    }

    fn on_start(&mut self, _: CoinType, block_height: usize) {
        self.start_height = block_height;
        info!(target: "callback", "Using `weakwallets` with dump folder: {} ...", &self.dump_folder.display());
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        self.start_height = block_height;
        // self.block_writer.write_all(block.save_csv(block_height).as_bytes()).unwrap();

        // let block_hash = utils::arr_to_hex_swapped(&block.header.hash);
        let r_arr = WeakWallets::block_r_arr(&block);
        for tx in block.txs {
            // self.tx_writer.write_all(tx.save_csv(&block_hash).as_bytes()).unwrap();
            let txid_str = utils::arr_to_hex_swapped(&tx.hash);

            for input in &tx.value.inputs {
                // self.txin_writer.write_all(input.save_csv(&txid_str).as_bytes()).unwrap();
                let tmp_sig = utils::arr_to_hex(&input.script_sig);
                if WeakWallets::repeat_r(tmp_sig, &r_arr){
                    self.ww_writer.write_all(input.save_csv(&txid_str).as_bytes()).unwrap();
                }
            }
            self.in_count += tx.value.in_count.value;

            // for (i, output) in tx.value.outputs.iter().enumerate() {
            //     self.txout_writer.write_all(output.save_csv(&txid_str, i).as_bytes()).unwrap();
            // }
            self.out_count += tx.value.out_count.value;
        }
        self.tx_count += block.tx_count.value;
    }

    fn on_complete(&mut self, block_height: usize) {
        self.end_height = block_height;

        // Keep in sync with c'tor
        // for f in vec!["blocks", "transactions", "tx_in", "tx_out"] {
        for f in vec!["weak_wallets"] {
            fs::rename(self.dump_folder.as_path().join(format!("{}.csv.tmp", f)),
                       self.dump_folder.as_path().join(format!("{}-{}-{}.csv", f, self.start_height, self.end_height)))
                .expect("Unable to rename tmp file!");
        }

        info!(target: "callback", "Done.\nDumped all {} blocks:\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, self.tx_count, self.in_count, self.out_count);
    }
}

impl Block {
    #[inline]
    fn save_csv(&self, block_height: usize) -> String {
        // (@hash, height, version, blocksize, @hashPrev, @hashMerkleRoot, nTime, nBits, nNonce)
        format!("{};{};{};{};{};{};{};{};{}\n",
            &utils::arr_to_hex_swapped(&self.header.hash),
            &block_height,
            &self.header.value.version,
            &self.blocksize,
            &utils::arr_to_hex_swapped(&self.header.value.prev_hash),
            &utils::arr_to_hex_swapped(&self.header.value.merkle_root),
            &self.header.value.timestamp,
            &self.header.value.bits,
            &self.header.value.nonce)
    }
}

impl Hashed<Tx> {
    #[inline]
    fn save_csv(&self, block_hash: &str) -> String {
        // (@txid, @hashBlock, version, lockTime)
        format!("{};{};{};{}\n",
            &utils::arr_to_hex_swapped(&self.hash),
            &block_hash,
            &self.value.tx_version,
            &self.value.tx_locktime)
    }
}

impl TxInput {
    #[inline]
    fn save_csv(&self, txid: &str) -> String {
        // (@txid, @hashPrevOut, indexPrevOut, scriptSig, sequence)
        format!("{};{};{};{};{}\n",
            &txid,
            &utils::arr_to_hex_swapped(&self.outpoint.txid),
            &self.outpoint.index,
            &utils::arr_to_hex(&self.script_sig),
            &self.seq_no)
    }
}

impl EvaluatedTxOut {
    #[inline]
    fn save_csv(&self, txid: &str, index: usize) -> String {
        // (@txid, indexOut, value, @scriptPubKey, address)
        format!("{};{};{};{};{}\n",
            &txid,
            &index,
            &self.out.value,
            &utils::arr_to_hex(&self.out.script_pubkey),
            &self.script.address)
    }
}
