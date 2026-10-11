//! The Lua scripts, byte for byte what the SDKs sent before the store moved into Core.
//!
//! Identical bytes give the same SHA-1, so a server that cached a script for an SDK's provider
//! serves Core from the same cache entry, and both sides keep one behavior. `keep.lua` is the
//! prelude of every script that writes a log.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);
pub(crate) const PROMOTE: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/promote.lua")
);
pub(crate) const STAGE: &str = include_str!("../../lua/stage.lua");

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
pub(crate) static PROMOTE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(PROMOTE));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scripts_are_the_ones_the_python_sdk_sends() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "7944489663d477b49e3f4f947933e15de3269718"
        );
        assert_eq!(
            PROMOTE_SCRIPT.get_hash(),
            "35a31f033f98bd6b321945a6b32f2f0fded1b22a"
        );
        assert_eq!(
            Script::new(STAGE).get_hash(),
            "e641e35793f1cad850988bfbd105d4787573f122"
        );
    }
}
