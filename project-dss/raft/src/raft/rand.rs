use rand::SeedableRng;

const N: usize = 250;
pub struct MyRngSeed(pub [u8; N]);
pub struct MyRng(MyRngSeed);

impl Default for MyRngSeed {
    fn default() -> MyRngSeed {
        MyRngSeed([0; N])
    }
}

impl AsMut<[u8]> for MyRngSeed {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl SeedableRng for MyRng {
    type Seed = MyRngSeed;

    fn from_seed(seed: MyRngSeed) -> MyRng {
        MyRng(seed)
    }
}
