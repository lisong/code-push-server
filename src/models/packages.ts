import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

export interface PackagesInterface extends Model {
    id: number;
    deployment_version_id: number;
    deployment_id: number;
    description: string;
    package_hash: string;
    blob_url: string;
    size: number;
    manifest_blob_url: string;
    release_method: string;
    label: string;
    original_label: string;
    original_deployment: string;
    released_by: number;
    is_mandatory: number;
    is_disabled: number;
    rollout: number;
    created_at: Date;
    updated_at: Date;
}

export const Packages = sequelize.define<PackagesInterface>(
    'Packages',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        deployment_version_id: DataTypes.INTEGER({ length: 10 }),
        deployment_id: DataTypes.INTEGER({ length: 10 }),
        description: DataTypes.STRING,
        package_hash: DataTypes.STRING,
        blob_url: DataTypes.STRING,
        size: DataTypes.INTEGER({ length: 10 }),
        manifest_blob_url: DataTypes.STRING,
        release_method: DataTypes.STRING,
        label: DataTypes.STRING,
        original_label: DataTypes.STRING,
        original_deployment: DataTypes.STRING,
        released_by: DataTypes.BIGINT({ length: 20 }),
        is_mandatory: DataTypes.INTEGER({ length: 3 }),
        is_disabled: DataTypes.INTEGER({ length: 3 }),
        rollout: DataTypes.INTEGER({ length: 3 }),
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'packages',
        underscored: true,
        paranoid: true,
    },
);
