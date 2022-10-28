#[derive(Debug)]
pub struct SparseSet {
    /// max len is set.len() / 2
    set: Box<[usize]>,
    len: usize,
}

impl SparseSet {
    pub fn new(cap: usize) -> Self {
        let v = vec![0; cap * 2];
        SparseSet {
            set: v.into_boxed_slice(),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, x: usize) {
        let batch = self.set.len() / 2;
        if x >= batch || self.len == batch {
            return;
        }

        let sparse = self.set[x + batch];
        let dense = self.set[sparse];

        if sparse < self.len && dense == x {
            return;
        }

        self.set[batch + x] = self.len;
        self.set[self.len] = x;
        self.len += 1;
    }

    pub fn pop(&mut self) -> Option<usize> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            Some(self.set[self.len])
        }
    }
}
