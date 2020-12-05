use crate::art::*;
use rand::Rng;
use rand::SeedableRng;

#[test]
fn set() {
    let dummy_value_1 = 0;
    let dummy_value_2 = 1;

    println!("insert into empty tree");
    {
        let mut trie = Art::empty();
        assert_eq!(None, trie.get("abc".as_bytes()));
        trie.insert("abc".as_bytes(), dummy_value_1);
        assert_eq!(Some(dummy_value_1), trie.get("abc".as_bytes()));
    }

    println!("insert into empty tree & replace");
    {
        let mut trie = Art::empty();
        trie.insert("abc".as_bytes(), dummy_value_1);
        trie.insert("abc".as_bytes(), dummy_value_2);
        assert_eq!(Some(dummy_value_2), trie.get("abc".as_bytes()));
    }

    println!("insert value s.t. existing value is a prefix");
    {
        let mut trie = Art::empty();
        let prefix_key = "abc".as_bytes();
        let key = "abcde".as_bytes();
        trie.insert(prefix_key, dummy_value_1);
        trie.insert(key, dummy_value_2);
        assert_eq!(Some(dummy_value_1), trie.get(prefix_key));
        assert_eq!(Some(dummy_value_2), trie.get(key));
    }

    println!("insert value s.t. new value is a prefix");
    {
        let mut trie = Art::empty();
        trie.insert("abcde".as_bytes(), dummy_value_1);
        trie.insert("abc".as_bytes(), dummy_value_2);
        assert_eq!(Some(dummy_value_1), trie.get("abcde".as_bytes()));
        assert_eq!(Some(dummy_value_2), trie.get("abc".as_bytes()));
    }

    println!("insert key s.t. it mismatches existing key");
    {
        let mut trie = Art::empty();
        let key1 = "aaaaa".as_bytes();
        let key2 = "aabaa".as_bytes();
        trie.insert(key1, dummy_value_1);
        trie.insert(key2, dummy_value_2);
        assert_eq!(Some(dummy_value_1), trie.get(key1));
        assert_eq!(Some(dummy_value_2), trie.get(key2));
    }

    println!("monte carlo");
    {
        const N: usize = 1000;
        let mut keys: Vec<[u8; 8]> = Vec::with_capacity(N);
        keys.resize_with(N, Default::default);
        let mut values: Vec<u64> = Vec::with_capacity(N);
        values.resize_with(N, Default::default);
        /* rng */
        let mut g = rand::thread_rng();

        for experiment in 0..10 {
            for i in 0..N {
                keys[i] = g.gen();
                values[i] = i as u64;
            }

            let mut m = Art::empty();

            for i in 0..N {
                // println!("insert {:?} {:?}", &keys[i], &values[i]);
                assert_eq!(None, m.insert(&keys[i], values[i]));
                // println!("{}", &m);
                // Check that we can get the value we just inserted.
                assert_eq!(Some(values[i]), m.get(&keys[i]));
                // Check that we can still get previous values.
                for j in 0..i {
                    let expected = Some(values[j]);
                    let found = m.get(&keys[j]);
                    assert_eq!(expected, found);
                }
            }
        }
    }
}

#[test]
fn delete_value() {
    let key0 = "aa".as_bytes();
    let int0 = 0;
    let key1 = "aaaa".as_bytes();
    let int1 = 1;
    let key2 = "aaaaaaa".as_bytes();
    let int2 = 2;
    let key3 = "aaaaaaaaaa".as_bytes();
    let int3 = 3;
    let key4 = "aaaaaaaba".as_bytes();
    let int4 = 4;
    let key5 = "aaaabaa".as_bytes();
    let int5 = 5;
    let key6 = "aaaabaaaaa".as_bytes();
    let int6 = 6;
    let key7 = "aaaaaaaaaaa".as_bytes();
    let int7 = 7;
    let key8 = "aaaaaaaaaab".as_bytes();
    let int8 = 8;
    let key9 = "aaaaaaaaaac".as_bytes();
    let int9 = 9;

    let setup = || -> Art {
        let mut m = Art::empty();

        m.insert(key0, int0);
        m.insert(key1, int1);
        m.insert(key2, int2);
        m.insert(key3, int3);
        m.insert(key4, int4);
        m.insert(key5, int5);
        m.insert(key6, int6);
        m.insert(key7, int7);
        m.insert(key8, int8);
        m.insert(key9, int9);

        m
    };

    /* The above statements construct the following tree:
     *
     *          (aa)
     *   $______/ |a
     *   /        |
     *  ()->0    (a)
     *   $______/ |a\___________b
     *   /        |             \
     *  ()->1   (aa)           (aa)
     *   $______/ |a\___b       |$\____a
     *   /        |     \       |      \
     *  ()->2 (aa$)->3 (a$)->4 ()->5 (aa)
     *                              $/   \a
     *                              /     \
     *                             ()->6  ()->7
     */

    println!("delete non existing value");
    {
        let mut m = setup();
        assert!(m.remove("aaaaa".as_bytes()) == None);
        assert!(m.remove("aaaaaa".as_bytes()) == None);
        assert!(m.remove("aaaab".as_bytes()) == None);
        assert!(m.remove("aaaaba".as_bytes()) == None);
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == Some(int3));
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));
    }

    println!("n_children == 0 && n_siblings == 0 (6)");
    {
        let mut m = setup();
        assert!(m.remove(key6) == Some(int6));
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == Some(int3));
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == None);
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));
    }

    println!("n_children == 0 && n_siblings == 1 (4)");
    {
        let mut m = setup();
        assert!(m.remove(key4) == Some(int4));
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == Some(int3));
        assert!(m.get(key4) == None);
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));
    }

    println!("n_children == 0 && n_siblings > 1 (7)");
    {
        let mut m = setup();
        assert!(m.remove(key7) == Some(int7));
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == Some(int3));
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == None);
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));
    }

    println!("n_children == 1 (0),(5)");
    {
        let mut m = setup();
        assert!(m.remove(key0) == Some(int0));
        assert!(m.get(key0) == None);
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == Some(int3));
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));

        assert!(m.remove(key5) == Some(int5));
        assert!(m.get(key0) == None);
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == Some(int3));
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == None);
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));
    }

    println!("n_children > 1 (3),(2),(1)");
    {
        let mut m = setup();
        assert!(m.remove(key3) == Some(int3));
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == Some(int2));
        assert!(m.get(key3) == None);
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));

        assert!(m.remove(key2) == Some(int2));
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == Some(int1));
        assert!(m.get(key2) == None);
        assert!(m.get(key3) == None);
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));

        assert!(m.remove(key1) == Some(int1));
        assert!(m.get(key0) == Some(int0));
        assert!(m.get(key1) == None);
        assert!(m.get(key2) == None);
        assert!(m.get(key3) == None);
        assert!(m.get(key4) == Some(int4));
        assert!(m.get(key5) == Some(int5));
        assert!(m.get(key6) == Some(int6));
        assert!(m.get(key7) == Some(int7));
        assert!(m.get(key8) == Some(int8));
        assert!(m.get(key9) == Some(int9));
    }
}

#[test]
fn monte_carlo_delete() {
    let mut m = Art::empty();
    let mut rng1 = rand::rngs::StdRng::from_seed([0; 32]);
    for i in 0..1000000 {
        let k: [u8; 10] = rng1.gen();
        let v = i;
        assert!(m.insert(&k, v) == None);
    }
    let mut rng2 = rand::rngs::StdRng::from_seed([0; 32]);
    for i in 0..1000000 {
        let k: [u8; 10] = rng2.gen();
        let get_res = m.get(&k);
        let del_res = m.remove(&k);
        assert!(m.get(&k) == None);
        assert!(get_res == del_res);
        assert!(del_res != None);
        assert!(del_res == Some(i));
    }
}

#[test]
fn full_range() {
    let int0 = 0;
    let int1 = 1;
    let int2 = 2;
    let int3 = 4;
    let int4 = 5;
    let int5 = 5;
    let int6 = 6;

    let mut m = Art::empty();

    m.insert("aa".as_bytes(), int0);
    m.insert("aaaa".as_bytes(), int1);
    m.insert("aaaaaaa".as_bytes(), int2);
    m.insert("aaaaaaaaaa".as_bytes(), int3);
    m.insert("aaaaaaaba".as_bytes(), int4);
    m.insert("aaaabaa".as_bytes(), int5);
    m.insert("aaaabaaaaa".as_bytes(), int6);

    /* The above statements construct the following tree:
     *
     *          (aa)
     *   $_____/ |a
     *   /       |
     *  ()->0   (a)
     *   $_____/ |a\____________b
     *   /       |              \
     *  ()->1   (aa)            (aa)
     *   $_____/ |a\___b         |$\____a
     *   /       |     \         |      \
     *  ()->2 (aa$)->3 (a$)->4 ()->5 (aa$)->6
     *
     */
    let found: Vec<u64> = m.range(..);
    let expected = vec![int0, int1, int2, int3, int4, int5, int6];
    assert_eq!(expected, found);
}

#[test]
fn count_full_range() {
    let n = 0x10000;
    let mut m = Art::empty();
    for i in 0..n {
        let key = format!("{:04X}", i);
        m.insert(key.as_bytes(), i as u64);
    }
    assert_eq!(n, m.range(..).len());
}

#[test]
fn partial_range() {
    println!("controlled test");
    {
        let int0 = 0;
        let int1 = 1;
        let int2 = 2;
        let int3 = 3;
        let int4 = 4;
        let int5 = 5;
        let int6 = 6;

        let mut m = Art::empty();

        m.insert("aa".as_bytes(), int0);
        m.insert("aaaa".as_bytes(), int1);
        m.insert("aaaaaaa".as_bytes(), int2);
        m.insert("aaaaaaaaaa".as_bytes(), int3);
        m.insert("aaaaaaaba".as_bytes(), int4);
        m.insert("aaaabaa".as_bytes(), int5);
        m.insert("aaaabaaaaa".as_bytes(), int6);

        /* The above statements construct the following tree:
         *
         *          (aa)
         *   $_____/ |a
         *   /       |
         *  ()->0   (a)
         *   $_____/ |a\____________b
         *   /       |              \
         *  ()->1   (aa)            (aa)
         *   $ ____/ |a\___b         |a\____$
         *    /      |     \         |      \
         *  ()->2 (aa$)->3 (a$)->4 ()->5 (aa$)->6
         *
         */
        let found: Vec<u64> = m.range("aaaaaaaaaa".as_bytes().."aaaabaaaaa".as_bytes());
        let expected = vec![int3, int4, int5];
        assert_eq!(expected, found);
    }

    println!("monte carlo");
    {
        let mut rng = rand::thread_rng();
        let mut m = Art::empty();
        const N: usize = 10000;
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(N);
        for i in 0..N {
            let key = format!("{:04X}", i).as_bytes().to_vec();
            m.insert(&key, i as u64);
            keys.push(key);
        }
        for experiment in 0..1000 {
            let start: usize = rng.gen::<u32>() as usize % N;
            let mut end: usize = 0;
            while end < start {
                end = rng.gen::<u32>() as usize % N;
            }
            let actual_n = m.range(keys[start].as_slice()..keys[end].as_slice()).len();
            assert_eq!(end - start, actual_n);
        }
    }
}
