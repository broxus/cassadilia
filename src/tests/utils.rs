#[cfg(test)]
pub mod directory_helpers {
    use std::path::Path;

    use anyhow::Result;

    pub fn count_directories_recursive(path: &Path) -> Result<usize> {
        let mut count = 0;

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                count += 1;
                count += count_directories_recursive(&path)?;
            }
        }

        Ok(count)
    }

    pub fn count_leaf_directories(path: &Path) -> Result<usize> {
        let mut count = 0;
        let mut has_subdirs = false;

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                has_subdirs = true;
                count += count_leaf_directories(&path)?;
            }
        }

        // If no subdirectories, this is a leaf directory
        if !has_subdirs {
            count = 1;
        }

        Ok(count)
    }

    #[derive(Debug)]
    pub struct DirectoryStructure {
        pub depth: usize,
        pub dirs_per_level: Vec<usize>,
    }

    pub fn analyze_directory_structure(root: &Path) -> Result<DirectoryStructure> {
        let mut dirs_per_level = Vec::new();
        let mut max_depth = 0;

        fn analyze_level(
            path: &Path,
            level: usize,
            dirs_per_level: &mut Vec<usize>,
            max_depth: &mut usize,
        ) -> Result<()> {
            if dirs_per_level.len() <= level {
                dirs_per_level.resize(level + 1, 0);
            }

            let mut dir_count = 0;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    dir_count += 1;
                    *max_depth = (*max_depth).max(level + 1);
                    analyze_level(&path, level + 1, dirs_per_level, max_depth)?;
                }
            }

            dirs_per_level[level] += dir_count;
            Ok(())
        }

        analyze_level(root, 0, &mut dirs_per_level, &mut max_depth)?;

        Ok(DirectoryStructure { depth: max_depth, dirs_per_level })
    }
}
